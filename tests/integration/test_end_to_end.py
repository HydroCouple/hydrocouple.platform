"""End to end: HTTP API, the real agent and local pool, runner processes, the real Solver."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import threading
import time

import jsonschema
import pytest

from conftest import DATA
from hydrocouple_platform.agent import Agent, AgentClient
from hydrocouple_platform.agent.adapters.local import LocalPoolAdapter
from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.jobs import EgressPolicy
from hydrocouple_platform.packaging import package_swmm_model
from hydrocouple_platform.runner.core import load_schema
from hydrocouple_platform.storage import sha256_file
from world import make_world

pytestmark = [pytest.mark.postgres, pytest.mark.engine]


@pytest.fixture(scope="module", autouse=True)
def engine_runs():
    """Skip when the installed engine cannot run on this CPU (e.g. an SVE-only build)."""
    pytest.importorskip("openswmm.engine")
    probe = subprocess.run(
        [sys.executable, "-c",
         "import sys; from openswmm.engine import Solver; "
         "s = Solver(sys.argv[1], '/dev/null', '/dev/null'); s.open(); s.close(); s.destroy()",
         str(DATA / "site_drainage_example.inp")],
        capture_output=True, timeout=120)
    if probe.returncode != 0:
        pytest.skip(f"the installed openswmm engine cannot run here (exit {probe.returncode})")


class Platform:
    """Runs the dispatcher, reconciler and agent loops in the background."""

    def __init__(self, services, world, server, artifact_dir, egress=EgressPolicy.SUMMARIES):
        self.services, self.world = services, world
        self.adapter = LocalPoolAdapter(artifact_dir / "agent-work", slots=2, keep_workdirs=True,
                                        grace_seconds=3)
        self.agent = Agent(AgentClient(server, world.credential), self.adapter, world.store,
                           world.store_uri, egress)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.errors: list[Exception] = []

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.services.dispatch()
                self.services.reconcile()
                self.agent.run_once()
            except Exception as exc:  # surfaced by __exit__ so the test fails loudly
                self.errors.append(exc)
                return
            time.sleep(0.1)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=30)
        for handle in self.adapter.handles():
            self.adapter.cancel(handle)
        if self.errors:
            raise self.errors[0]


def submit(client, world, revision_id, key):
    return client.submit_run({
        "project_id": world.project_id, "revision_id": revision_id, "workflow": "swmm.simulation",
        "workflow_version": "1", "target_id": world.target_id,
        "parameters": {"progress_interval_seconds": 0.2}}, idempotency_key=key)


def longer(model, days: int, dest):
    """A copy of ``model`` whose simulation runs ``days`` days."""
    shutil.copytree(model.parent, dest, dirs_exist_ok=True)
    text = (dest / model.name).read_text()
    text = re.sub(r"END_DATE\s+\S+", f"END_DATE             01/{1 + days:02d}/1998", text)
    (dest / model.name).write_text(text)
    return dest / model.name


def test_package_run_and_download_with_numerical_parity(services, artifact_dir, server,
                                                         make_token):
    world = make_world(services, artifact_dir)
    with Platform(services, world, server, artifact_dir), \
            PlatformClient(server, make_token("alice")) as client:
        package = package_swmm_model(DATA / "external_rain" / "model.inp",
                                     artifact_dir / "package")
        revision = client.upload_revision(package, project_id=world.project_id,
                                          target_id=world.target_id)
        accepted = submit(client, world, revision["revision_id"], "e2e-parity-1")
        assert submit(client, world, revision["revision_id"], "e2e-parity-1")["job_id"] == \
            accepted["job_id"]
        view = client.wait_for_job(accepted["job_id"], timeout=180)
        assert view["state"] == "SUCCEEDED", view
        assert view["progress"]["completed"] == view["progress"]["total"]

        results = client.results(accepted["run_id"])
        quality = results["quality"]["continuity_errors"]
        assert abs(quality["runoff"]) < 0.01 and abs(quality["routing"]) < 0.01
        prov = results["provenance"]
        assert prov["inputs"]["model/rain/2-yr storm.dat"] == sha256_file(
            DATA / "external_rain" / "rain" / "2-yr storm.dat")
        assert prov["runtime"]["engine"]
        downloaded = client.download(accepted["run_id"], ["model.out", "run.json", "model.rpt"],
                                     artifact_dir / "downloaded")
        jsonschema.validate(json.loads(downloaded["run.json"].read_text()),
                            load_schema("run-1.0.json"))
        (artifact_dir / "results.json").write_text(json.dumps(results, indent=2))

    # Parity: the same model run directly with the engine, on the original files.
    direct = artifact_dir / "direct"
    shutil.copytree(DATA / "external_rain", direct)
    subprocess.run([sys.executable, "-c",
                    "import sys; from openswmm.engine import Solver; "
                    "s = Solver(sys.argv[1], sys.argv[2], sys.argv[3]); s.run(); s.destroy()",
                    str(direct / "model.inp"), str(direct / "model.rpt"),
                    str(direct / "model.out")], check=True, timeout=120)
    assert sha256_file(direct / "model.out") == sha256_file(downloaded["model.out"])


def test_cancel_a_running_simulation(services, artifact_dir, server, make_token):
    world = make_world(services, artifact_dir)
    model = longer(DATA / "site_drainage_example.inp", 28, artifact_dir / "long")
    with Platform(services, world, server, artifact_dir) as platform, \
            PlatformClient(server, make_token("alice")) as client:
        revision = client.upload_revision(package_swmm_model(model, artifact_dir / "package"),
                                          project_id=world.project_id, target_id=world.target_id)
        accepted = submit(client, world, revision["revision_id"], "e2e-cancel-1")
        deadline = time.monotonic() + 60
        while (client.job(accepted["job_id"])["progress"] or {}).get("completed", 0) <= 0:
            assert time.monotonic() < deadline, "the run never reported progress"
            time.sleep(0.1)
        client.cancel(accepted["job_id"])
        view = client.wait_for_job(accepted["job_id"], timeout=60)
        assert view["state"] == "CANCELLED", view
        assert view["progress"]["completed"] < view["progress"]["total"]
        assert all(p.process.poll() is not None for p in platform.adapter._procs.values())


def test_an_invalid_model_fails_without_retries(services, artifact_dir, server, make_token):
    world = make_world(services, artifact_dir)
    bad = artifact_dir / "bad" / "model.inp"
    bad.parent.mkdir()
    text = (DATA / "site_drainage_example.inp").read_text()
    # A conduit to an undefined node: the engine refuses it (ERROR 209).
    bad.write_text(text.replace("C1               J1               J5 ",
                                "C1               J1               NO_SUCH_NODE ", 1))
    with Platform(services, world, server, artifact_dir), \
            PlatformClient(server, make_token("alice")) as client:
        revision = client.upload_revision(package_swmm_model(bad, artifact_dir / "package"),
                                          project_id=world.project_id, target_id=world.target_id)
        accepted = submit(client, world, revision["revision_id"], "e2e-invalid-1")
        view = client.wait_for_job(accepted["job_id"], timeout=120)
        assert view["state"] == "FAILED", view
        assert view["failure"]["class"] == "invalid_input"
        assert len(view["attempts"]) == 1
        assert view["failure"]["exit_code"] == 10
        assert "ERROR 209" in view["failure"]["diagnostics"]["message"]


def test_work_survives_a_control_plane_outage(services, artifact_dir, server, make_token):
    world = make_world(services, artifact_dir)
    model = longer(DATA / "site_drainage_example.inp", 6, artifact_dir / "medium")
    with Platform(services, world, server, artifact_dir) as platform, \
            PlatformClient(server, make_token("alice")) as client:
        revision = client.upload_revision(package_swmm_model(model, artifact_dir / "package"),
                                          project_id=world.project_id, target_id=world.target_id)
        accepted = submit(client, world, revision["revision_id"], "e2e-outage-1")
        deadline = time.monotonic() + 60
        while client.job(accepted["job_id"])["state"] not in ("RUNNING", "SUCCEEDED"):
            assert time.monotonic() < deadline
            time.sleep(0.05)
        # The agent loses the control plane; the runner keeps going and events buffer.
        real = platform.agent.client
        platform.agent.client = AgentClient("http://127.0.0.1:9", world.credential, timeout=0.5)
        time.sleep(2.0)
        platform.agent.client = real
        view = client.wait_for_job(accepted["job_id"], timeout=120)
        assert view["state"] == "SUCCEEDED", view
        assert len(view["attempts"]) == 1
