"""The M3b gate: studies on a real evaluator pool, compared with a local run.

The control plane runs over HTTP, the real agent drives the local pool, each pool
task is a runner process holding an ``openswmm_gymnasium`` env, and the study is
driven by ``run_platform_study``. The reference is
``openswmm_gymnasium.optimize.run_study`` in this process: the same optimizer,
seed and env, evaluated locally. Both runs keep their ``evaluations.jsonl`` and
``result.json`` under ``test_artifacts/`` for review.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import signal
import threading
import time

import pytest
from sqlalchemy import func, select

from conftest import DATA
from hydrocouple_platform.agent import Agent, AgentClient
from hydrocouple_platform.agent.adapters.local import LocalPoolAdapter
from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.jobs import EgressPolicy
from hydrocouple_platform.db.models import EvaluationChunk, Study
from hydrocouple_platform.gym import run_platform_study
from hydrocouple_platform.packaging import package_swmm_model
from world import make_world

pytestmark = [pytest.mark.postgres, pytest.mark.engine]

MODEL = DATA / "twin_tank_heavy" / "model.inp"


@pytest.fixture(scope="module", autouse=True)
def gymnasium_runs():
    """Skip without openswmm_gymnasium, or with an engine it cannot use."""
    pytest.importorskip("openswmm_gymnasium.optimize")
    from openswmm_gymnasium.optimize import Evaluator, design_dimensions
    from openswmm_gymnasium.spec.config import EnvConfig

    try:
        config = EnvConfig.model_validate(env_config(str(MODEL)))
        Evaluator(config, design_dimensions(config)).close()
    except Exception as exc:
        pytest.skip(f"openswmm_gymnasium cannot build the test env here: {exc}")


def env_config(inp_path: str) -> dict:
    return {
        "env_type": "cip", "inp_path": inp_path,
        "design_factories": [
            {"kind": "node_max_depth",
             "params": {"node_ids": ["T1", "T2"], "low": 2.0, "high": 4.0}},
            {"kind": "link_diameter",
             "params": {"link_ids": ["OUT"], "low": 0.5, "high": 3.0}}],
        "observations": {"node_depths": ["T1", "T2"]},
        "reward_terms": [{"kind": "flooding_volume", "params": {}},
                         {"kind": "peak_outflow", "params": {"link_ids": ["OUT"]}}],
    }


class Platform:
    """The dispatcher, reconciler and agent loops, in the background."""

    def __init__(self, services, world, server, artifact_dir, slots: int):
        self.services = services
        self.adapter = LocalPoolAdapter(artifact_dir / "agent-work", slots=slots,
                                        keep_workdirs=True, grace_seconds=3)
        self.agent = Agent(AgentClient(server, world.credential), self.adapter, world.store,
                           world.store_uri, EgressPolicy.SUMMARIES)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.errors: list[Exception] = []

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.services.dispatch()
                self.services.reconcile()
                self.agent.run_once()
            except Exception as exc:
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
        self.agent.close()
        if self.errors:
            raise self.errors[0]


def local_reference(opt, artifact_dir):
    from openswmm_gymnasium.optimize import run_study
    from openswmm_gymnasium.spec.config import EnvConfig

    model = artifact_dir / "local-model" / "model.inp"
    model.parent.mkdir(parents=True)
    shutil.copyfile(MODEL, model)
    config = EnvConfig.model_validate(env_config(str(model)))
    if opt.algorithm != "random_search" and opt.seed is not None:
        random.seed(opt.seed)  # run_study leaves Platypus unseeded; seed it as the platform does
    return config, run_study(config, opt, artifact_dir / "local")


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def assert_same_evaluations(local, remote):
    assert len(local) == len(remote)
    for a, b in zip(local, remote, strict=True):
        assert a["evaluation"] == b["evaluation"]
        assert a["decisions"] == b["decisions"]
        assert a.get("failed", False) == b.get("failed", False)
        for name, value in a["objectives"].items():
            assert b["objectives"][name] == pytest.approx(value, rel=1e-12, abs=1e-9), \
                (a["evaluation"], name)


def upload(client, world, artifact_dir):
    package = package_swmm_model(MODEL, artifact_dir / "package")
    return client.upload_revision(package, project_id=world.project_id,
                                  target_id=world.target_id)


def test_a_1000_evaluation_study_matches_a_local_run(services, artifact_dir, server,
                                                     make_token):
    from openswmm_gymnasium.optimize import OptimizationConfig

    opt = OptimizationConfig(algorithm="random_search", budget=1000, seed=11, workers=0,
                             on_failure="penalty")
    config, reference = local_reference(opt, artifact_dir)

    world = make_world(services, artifact_dir)
    killed: list[str] = []
    stop = threading.Event()
    with Platform(services, world, server, artifact_dir, slots=3) as platform, \
            PlatformClient(server, make_token("alice")) as client:
        revision = upload(client, world, artifact_dir)

        def kill_a_task_mid_chunk():
            # Once a third of the study is done, kill a task while it holds a chunk.
            while not stop.is_set():
                with services.tx() as s:
                    done = s.scalar(select(func.coalesce(func.sum(Study.evaluations_completed),
                                                         0)))
                    holder = s.scalar(select(EvaluationChunk.holder_attempt_id).where(
                        EvaluationChunk.state == "LEASED").limit(1))
                proc = platform.adapter._procs.get(holder) if holder else None
                if done >= 330 and proc is not None and proc.process.poll() is None:
                    os.killpg(proc.process.pid, signal.SIGKILL)
                    killed.append(holder)
                    return
                time.sleep(0.02)

        killer = threading.Thread(target=kill_a_task_mid_chunk, daemon=True)
        killer.start()
        started = time.monotonic()
        # Batches of 60 make chunks of 10 candidates, long enough to be caught mid-chunk.
        result = run_platform_study(
            config, opt, artifact_dir / "platform", client=client,
            project_id=world.project_id, revision_id=revision["revision_id"],
            target_id=world.target_id, inp_path="model/model.inp", pool_size=3,
            batch_size=60)
        elapsed = time.monotonic() - started
        stop.set()
        study_id = result["execution"]["platform"]["study_id"]
        server_results = client.study_results(study_id)
        manifest = client.study_manifest(study_id)
        deadline = time.monotonic() + 60
        while any(p["state"] not in ("SUCCEEDED", "FAILED", "CANCELLED")
                  for p in client.study(study_id)["pool"]):
            assert time.monotonic() < deadline, "the pool did not finish after the study closed"
            time.sleep(0.25)
        study = client.study(study_id)
        manifest = client.study_manifest(study_id)

    assert killed, "no pool task was running to kill"
    local = read_jsonl(artifact_dir / "local" / "evaluations.jsonl")
    remote = read_jsonl(artifact_dir / "platform" / "evaluations.jsonl")
    assert len(remote) == 1000
    assert_same_evaluations(local, remote)
    assert [p["evaluation"] for p in result["pareto"]] == \
        [p["evaluation"] for p in reference["pareto"]]
    assert result["best"]["evaluation"] == reference["best"]["evaluation"]
    assert len(reference["pareto"]) > 3, "the test problem should have a real trade-off"

    # The control plane's own view agrees, and its lineage is complete.
    assert study["state"] == "COMPLETED" and study["evaluations_completed"] == 1000
    assert [p["evaluation"] for p in server_results["pareto"]] == \
        [p["evaluation"] for p in reference["pareto"]]
    redelivered = [c for c in manifest["chunks"] if c["deliveries"] > 1]
    assert redelivered, "the killed task's chunk should have been leased again"
    assert all(c["attempt_id"] != killed[0] for c in redelivered)
    for chunk in manifest["chunks"]:
        assert chunk["state"] == "DONE"
        assert world.store.stat(chunk["object"]["key"])[1] == chunk["object"]["sha256"]
    attempts = [a for p in manifest["pool"] for a in p["attempts"]]
    assert any(a["state"] == "FAILED" for a in attempts)  # the killed one
    assert all(a["attempt_manifest_sha256"] for a in attempts if a["state"] == "SUCCEEDED")
    (artifact_dir / "study-manifest.json").write_text(json.dumps(manifest, indent=2))
    (artifact_dir / "timing.json").write_text(json.dumps({"platform_seconds": elapsed}))


def test_nsga2_on_the_platform_matches_the_same_seeded_local_run(services, artifact_dir, server,
                                                                 make_token):
    pytest.importorskip("platypus")
    from openswmm_gymnasium.optimize import OptimizationConfig

    opt = OptimizationConfig(algorithm="nsga2", budget=200, population_size=20, seed=5,
                             workers=0, on_failure="penalty")
    config, reference = local_reference(opt, artifact_dir)
    world = make_world(services, artifact_dir)
    with Platform(services, world, server, artifact_dir, slots=2), \
            PlatformClient(server, make_token("alice")) as client:
        revision = upload(client, world, artifact_dir)
        result = run_platform_study(
            config, opt, artifact_dir / "platform", client=client,
            project_id=world.project_id, revision_id=revision["revision_id"],
            target_id=world.target_id, inp_path="model/model.inp", pool_size=2)

    assert_same_evaluations(read_jsonl(artifact_dir / "local" / "evaluations.jsonl"),
                            read_jsonl(artifact_dir / "platform" / "evaluations.jsonl"))
    assert [p["decisions"] for p in result["pareto"]] == \
        [p["decisions"] for p in reference["pareto"]]
