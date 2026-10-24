"""Start a private PostgreSQL cluster for tests.

Set ``HCP_TEST_DATABASE_URL`` to use an existing server, or ``HCP_TEST_PG_BIN``
to a directory holding ``initdb`` and ``pg_ctl``. Without either, the
``pgserver`` package's bundled binaries are used when it is installed.

The cluster's data directory is infrastructure, not a test output, so it lives
under the system temporary directory and is removed on stop.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


def find_pg_bin() -> Path | None:
    explicit = os.environ.get("HCP_TEST_PG_BIN")
    if explicit:
        return Path(explicit)
    try:
        import pgserver  # type: ignore[import-not-found]
    except ImportError:
        return None
    candidate = Path(pgserver.__file__).parent / "pginstall" / "bin"
    return candidate if (candidate / "initdb").exists() else None


@dataclass
class Cluster:
    url: str
    _stop: list[str] | None = None
    _root: Path | None = None
    _as: list[str] | None = None

    def stop(self) -> None:
        if self._stop:
            subprocess.run([*(self._as or []), *self._stop], check=False, capture_output=True)
        if self._root:
            shutil.rmtree(self._root, ignore_errors=True)


def start_cluster(port: int = 54329) -> Cluster:
    url = os.environ.get("HCP_TEST_DATABASE_URL")
    if url:
        return Cluster(url=url)
    pg_bin = find_pg_bin()
    if pg_bin is None:
        raise RuntimeError("set HCP_TEST_DATABASE_URL or HCP_TEST_PG_BIN to run postgres tests")

    root = Path(tempfile.mkdtemp(prefix="hcp-pg-"))
    data, sock = root / "data", root / "sock"
    data.mkdir()
    sock.mkdir()
    run_as: list[str] = []
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        # initdb refuses to run as root; use the postgres account when present.
        root.chmod(0o755)
        shutil.chown(data, "postgres")
        shutil.chown(sock, "postgres")
        run_as = ["runuser", "-u", "postgres", "--"]
    subprocess.run(
        [*run_as, str(pg_bin / "initdb"), "-D", str(data), "-U", "hcp", "--auth=trust",
         "--encoding=UTF8", "--no-locale"],
        check=True, capture_output=True,
    )
    options = f"-k {sock} -p {port} -c listen_addresses='' -c fsync=off -c max_connections=200"
    subprocess.run(
        [*run_as, str(pg_bin / "pg_ctl"), "-D", str(data), "-o", options,
         "-l", str(sock / "postgres.log"), "-w", "start"],
        check=True, capture_output=True,
    )
    return Cluster(
        url=f"postgresql+psycopg://hcp@/postgres?host={sock}&port={port}",
        _stop=[str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"],
        _root=root,
        _as=run_as,
    )
