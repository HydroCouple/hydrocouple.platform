"""Shared fixtures.

Postgres tests run against a real server (see ``tests/pgcluster.py``); each test
gets its own database cloned from a migrated template. Engine tests run the
real openswmm Solver. Files a test produces are written under
``test_artifacts/<module>/<test>/`` so they can be reviewed after the run.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(__file__).parent / "data"
# Override where test files go (for example on a filesystem that forbids deletes).
ARTIFACTS = Path(os.environ.get("HCP_TEST_ARTIFACTS", ROOT / "test_artifacts"))
sys.path.insert(0, str(Path(__file__).parent))

ISSUER, AUDIENCE = "https://idp.example.test", "hydrocouple-platform"


@pytest.fixture
def artifact_dir(request) -> Path:
    """A clean, reviewable folder for this test's files."""
    module = Path(request.node.fspath).stem
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", request.node.name)
    path = ARTIFACTS / module / name
    shutil.rmtree(path, ignore_errors=True)
    if path.exists():
        raise RuntimeError(f"cannot clear {path}; set HCP_TEST_ARTIFACTS to a writable folder")
    path.mkdir(parents=True)
    return path


# -- database -------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_template():
    from sqlalchemy import create_engine, text

    from hydrocouple_platform.db import upgrade
    from pgcluster import start_cluster

    try:
        cluster = start_cluster()
    except RuntimeError as exc:
        pytest.skip(str(exc))
    admin = create_engine(cluster.url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text("DROP DATABASE IF EXISTS hcp_template"))
        conn.execute(text("CREATE DATABASE hcp_template"))
    template_url = cluster.url.replace("/postgres?", "/hcp_template?")
    upgrade(template_url)
    yield cluster, admin
    admin.dispose()
    cluster.stop()


@pytest.fixture
def database_url(pg_template):
    from sqlalchemy import text

    cluster, admin = pg_template
    name = "t_" + uuid.uuid4().hex[:12]
    with admin.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {name} TEMPLATE hcp_template"))
    yield cluster.url.replace("/postgres?", f"/{name}?")


# -- identity -------------------------------------------------------------------------


@pytest.fixture(scope="session")
def signing_key():
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def jwks_file(signing_key, tmp_path_factory) -> Path:
    import jwt

    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(signing_key.public_key()))
    jwk.update({"kid": "test-key", "use": "sig", "alg": "RS256"})
    path = tmp_path_factory.mktemp("jwks") / "jwks.json"
    path.write_text(json.dumps({"keys": [jwk]}))
    return path


@pytest.fixture
def make_token(signing_key):
    import jwt

    def make(subject: str, *, audience: str = AUDIENCE, issuer: str = ISSUER,
             expires_in: int = 600) -> str:
        now = datetime.now(timezone.utc)
        return jwt.encode(
            {"iss": issuer, "aud": audience, "sub": subject, "email": f"{subject}@example.test",
             "iat": now, "exp": now + timedelta(seconds=expires_in)},
            signing_key, algorithm="RS256", headers={"kid": "test-key"})

    return make


# -- services -------------------------------------------------------------------------


class Clock:
    """A controllable clock for lease, deadline and expiry tests."""

    def __init__(self):
        self.offset = timedelta(0)

    def __call__(self) -> datetime:
        return datetime.now(timezone.utc) + self.offset

    def advance(self, seconds: float) -> None:
        self.offset += timedelta(seconds=seconds)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def settings(database_url, jwks_file):
    from hydrocouple_platform.config import ControlPlaneSettings

    return ControlPlaneSettings(
        database_url=database_url, jwt_issuer=ISSUER, jwt_audience=AUDIENCE,
        jwks_file=str(jwks_file), attempt_token_secret="s" * 48, lease_seconds=30,
        abandon_after_seconds=120, heartbeat_interval_seconds=0.2,
    )


@pytest.fixture
def services(settings, clock):
    from hydrocouple_platform.db import make_engine, make_sessionmaker
    from hydrocouple_platform.services import PlatformServices

    engine = make_engine(settings.database_url)
    yield PlatformServices(make_sessionmaker(engine), settings, clock)
    engine.dispose()


@pytest.fixture
def server(settings, services):
    """The real API on a loopback port, sharing ``services`` (and its clock)."""
    import uvicorn

    from hydrocouple_platform.api import create_app

    app = create_app(settings, services=services)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not srv.started:
        if time.monotonic() > deadline:
            raise RuntimeError("the test server did not start")
        time.sleep(0.05)
    port = srv.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    srv.should_exit = True
    thread.join(timeout=10)
