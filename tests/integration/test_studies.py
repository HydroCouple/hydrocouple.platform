"""Studies and evaluator pools against PostgreSQL, with scripted evaluator tasks.

The tasks here call the agent routes in process and compute costs from a
formula, so these tests cover the control plane's rules (fencing, re-leasing,
failure policies, budgets, approvals, isolation) without the engine.
``test_studies_engine.py`` runs real evaluators.
"""

from __future__ import annotations

import json
import random

import pytest

from conftest import DATA
from hydrocouple_platform.contracts.agent import AttemptOutcome, AttemptResult
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.jobs import FailureClass, JobClass
from hydrocouple_platform.contracts.requests import AgentSessionCreate, Budget, RunRequest
from hydrocouple_platform.contracts.studies import (
    BatchSubmit,
    Dimension,
    EvaluationCompletion,
    EvaluationLeaseRequest,
    Objective,
    ResultObject,
    StudyCreate,
)
from hydrocouple_platform.db.models import EvaluationChunk, Job
from hydrocouple_platform.services.studies import pareto_indices
from hydrocouple_platform.storage import sha256_file
from world import local_capabilities, make_world, ready_revision

pytestmark = pytest.mark.postgres

POOL_CAPS = local_capabilities(job_classes=[JobClass.SINGLE, JobClass.EVALUATOR_POOL])
MODEL = "model/model.inp"
DIMS = [Dimension(label="node_max_depth:T1", low=2.0, high=4.0),
        Dimension(label="node_max_depth:T2", low=2.0, high=4.0),
        Dimension(label="link_diameter:OUT", low=0.5, high=3.0)]
OBJECTIVES = [Objective(name="flooding_volume", direction="minimize"),
              Objective(name="peak_outflow", direction="minimize")]


def env_config() -> dict:
    return {
        "env_type": "cip", "inp_path": MODEL,
        "design_factories": [
            {"kind": "node_max_depth",
             "params": {"node_ids": ["T1", "T2"], "low": 2.0, "high": 4.0}},
            {"kind": "link_diameter",
             "params": {"link_ids": ["OUT"], "low": 0.5, "high": 3.0}}],
        "observations": {"node_depths": ["T1", "T2"]},
        "reward_terms": [{"kind": "flooding_volume", "params": {}},
                         {"kind": "peak_outflow", "params": {"link_ids": ["OUT"]}}],
    }


def costs_of(vector: list[float]) -> list[float]:
    """A deterministic stand-in for an episode, trading flooding against peak outflow."""
    t1, t2, d = vector
    return [round((4 - t1) * (4 - t2) * (3 - d) * 100, 9), round(d * 4 + t1 * 0.1, 9)]


def candidates(n: int, seed: int = 1) -> list[list[float]]:
    rng = random.Random(seed)
    return [[rng.uniform(2, 4), rng.uniform(2, 4), rng.uniform(0.5, 3)] for _ in range(n)]


@pytest.fixture
def setup(services, artifact_dir):
    world = make_world(services, artifact_dir)
    revision = ready_revision(world, DATA / "twin_tank_heavy" / "model.inp")
    world.poll(free_slots=0, caps=POOL_CAPS)  # the agent reports it runs evaluator pools
    return world, revision


def create(world, revision, key="study-key-0001", principal=None, parameters=None, **kw):
    body = StudyCreate(
        project_id=world.project_id, revision_id=revision["revision_id"],
        target_id=world.target_id,
        parameters=parameters or {"env_config": env_config(), "evaluation_timeout_seconds": 5},
        dimensions=DIMS, objectives=OBJECTIVES, seed=7,
        max_evaluations=kw.pop("max_evaluations", 100), pool_size=kw.pop("pool_size", 2), **kw)
    return world.services.create_study(principal or world.admin, body, key)


class Task:
    """One evaluator task attempt, scripted against the agent routes."""

    def __init__(self, world, lease):
        self.world, self.lease = world, lease

    @property
    def attempt_id(self) -> str:
        return self.lease.attempt_id

    def lease_chunk(self):
        return self.world.services.evaluation_lease(
            self.world.target_id, self.lease.attempt_token,
            EvaluationLeaseRequest(attempt_id=self.attempt_id,
                                   fencing_token=self.lease.fencing_token))

    def complete(self, chunk, results=None, lease_number=None):
        lease_number = lease_number or chunk.lease_number
        results = results or [{"costs": costs_of(v), "steps": 1} for v in chunk.candidates]
        prefix = f"{self.lease.spec.outputs.storage_prefix}{self.attempt_id}/evaluations/"
        local = self.world.root / "task-records" / f"{chunk.chunk_id}-{lease_number}.jsonl"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text("".join(json.dumps(r) + "\n" for r in results))
        key = f"{prefix}{chunk.chunk_id}-{lease_number}.jsonl"
        self.world.store.put_file(key, local)
        return self.world.services.evaluation_complete(
            self.world.target_id, self.lease.attempt_token, EvaluationCompletion(
                attempt_id=self.attempt_id, fencing_token=self.lease.fencing_token,
                chunk_id=chunk.chunk_id, lease_number=lease_number, results=results,
                object=ResultObject(key=key, sha256=sha256_file(local),
                                    size=local.stat().st_size)))

    def drain(self) -> int:
        done = 0
        while True:
            response = self.lease_chunk()
            if response.chunk is None:
                return done
            assert self.complete(response.chunk).accepted
            done += len(response.chunk.candidates)


def start_tasks(world, slots: int = 4) -> list[Task]:
    world.services.dispatch()
    response = world.poll(free_slots=slots, caps=POOL_CAPS)
    return [Task(world, lease) for lease in response.leases]


# -- the happy path ------------------------------------------------------------------


def test_a_study_evaluates_batches_on_its_pool_with_lineage(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision)
    assert study.state == "OPEN" and study.pool == []
    vectors = candidates(10)
    batch = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=vectors),
                             "batch-key-0001")
    assert batch.state == "QUEUED" and batch.size == 10
    assert len(svc.get_study(world.admin, study.study_id).pool) == 2  # topped up to pool_size

    tasks = start_tasks(world)
    assert len(tasks) == 2
    assert all(t.lease.spec.resources.job_class == JobClass.EVALUATOR_POOL for t in tasks)
    assert tasks[0].lease.spec.workload.parameters["study"]["study_id"] == study.study_id
    while sum(t.drain() for t in tasks):
        pass

    view = svc.get_batch(world.admin, study.study_id, batch.batch_id)
    assert view.state == "DONE" and view.completed == 10
    assert [r.evaluation for r in view.results] == list(range(1, 11))
    assert [r.costs for r in view.results] == [costs_of(v) for v in vectors]

    second = svc.submit_batch(world.admin, study.study_id,
                              BatchSubmit(candidates=candidates(5, seed=2)), "batch-key-0002")
    while sum(t.drain() for t in tasks):
        pass
    assert svc.get_batch(world.admin, study.study_id, second.batch_id).results[0].evaluation == 11

    closed = svc.close_study(world.admin, study.study_id)
    assert closed.state == "COMPLETED"
    assert all(t.lease_chunk().done for t in tasks)  # each task is told to finish

    listed = svc.list_evaluations(world.admin, study.study_id, after=0, limit=7)
    assert [r["evaluation"] for r in listed["evaluations"]] == list(range(1, 8))
    rest = svc.list_evaluations(world.admin, study.study_id, after=listed["next_after"])
    assert [r["evaluation"] for r in rest["evaluations"]] == list(range(8, 16))
    assert rest["complete"]
    first = listed["evaluations"][0]
    assert first["decisions"] == vectors[0]
    assert first["attempt_id"] in {t.attempt_id for t in tasks} and first["lease_number"] == 1

    results = svc.study_results(world.admin, study.study_id)
    everything = [(r["evaluation"], r["costs"]) for r in
                  listed["evaluations"] + rest["evaluations"]]
    expected = [everything[i][0] for i in pareto_indices(
        [(i, c) for i, (_, c) in enumerate(everything)])]
    assert [r["evaluation"] for r in results["pareto"]] == expected
    assert results["evaluations_count"] == 15 and results["failed_count"] == 0

    manifest = svc.study_manifest(world.admin, study.study_id)
    assert manifest["counts"] == {"submitted": 15, "completed": 15, "failed": 0}
    for chunk in manifest["chunks"]:
        assert world.store.stat(chunk["object"]["key"])[1] == chunk["object"]["sha256"]
    assert {p["job_id"] for p in manifest["pool"]} == {t.lease.job_id for t in tasks}
    assert manifest["revision_manifest_sha256"] == revision["manifest_sha256"]


def test_pareto_matches_the_gymnasium_rule():
    points = [(0, [1, 5]), (1, [2, 2]), (2, [3, 1]), (3, [2, 2]), (4, [4, 4]), (5, [1, 6]),
              (6, [5, 0.5])]
    # (4,4) is dominated by (2,2); (1,6) by (1,5); equal points do not dominate each other.
    assert pareto_indices(points) == [0, 1, 2, 3, 6]

    rng = random.Random(3)
    cloud = [(i, [rng.random(), rng.random(), rng.random()]) for i in range(300)]
    brute = [i for i, a in cloud if not any(
        all(x <= y for x, y in zip(b, a, strict=True)) and
        any(x < y for x, y in zip(b, a, strict=True)) for _, b in cloud)]
    assert pareto_indices(cloud) == brute


# -- fencing -----------------------------------------------------------------------


def test_a_late_result_from_an_earlier_lease_is_discarded(setup, clock):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1)
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(2)),
                     "batch-key-0001")
    (slow,) = start_tasks(world)
    first = slow.lease_chunk().chunk
    assert first.lease_number == 1

    clock.advance(5 * len(first.candidates) + 31)  # past the chunk's lease
    counts = svc.reconcile()
    assert counts["chunk_leases_expired"] >= 1

    # The same task (or a replacement) leases the chunk again; the old lease is fenced off.
    again = slow.lease_chunk().chunk
    assert again.chunk_id == first.chunk_id and again.lease_number == 2
    late = slow.complete(first, lease_number=1)
    assert not late.accepted and "discarded" in late.reason
    assert slow.complete(again).accepted
    assert slow.complete(again).accepted  # a repeated report of the same result is idempotent
    view = svc.get_study(world.admin, study.study_id)
    assert view.evaluations_completed == len(first.candidates)


def test_an_attempt_that_ends_returns_its_chunks(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1)
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1)),
                     "batch-key-0001")
    (task,) = start_tasks(world)
    chunk = task.lease_chunk().chunk
    ack = svc.agent_complete(world.target_id, task.lease.attempt_token, AttemptResult(
        attempt_id=task.attempt_id, fencing_token=task.lease.fencing_token,
        outcome=AttemptOutcome.FAILED, failure_class=FailureClass.ENGINE_CRASH))
    assert ack.retry_scheduled
    with svc.tx() as s:
        row = s.get(EvaluationChunk, chunk.chunk_id)
        assert row.state == "PENDING" and row.holder_attempt_id is None
    (retry,) = start_tasks(world)
    assert retry.lease.job_id == task.lease.job_id and retry.lease.fencing_token == 2
    again = retry.lease_chunk().chunk
    assert again.chunk_id == chunk.chunk_id and again.lease_number == 2
    with pytest.raises(PlatformError) as stale:  # the ended attempt can no longer report
        task.complete(chunk)
    assert stale.value.code == ErrorCode.STALE_ATTEMPT
    assert retry.complete(again).accepted


# -- failed candidates ------------------------------------------------------------------


def test_the_penalty_policy_records_failed_candidates(setup, clock):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1, on_failure="penalty", failure_penalty=1e6)
    vectors = candidates(2)
    batch = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=vectors),
                             "batch-key-0001")
    (task,) = start_tasks(world)
    chunk = task.lease_chunk().chunk  # the first candidate's chunk fails
    failure = {"costs": None, "steps": 0, "error": "RuntimeError: the engine failed"}
    assert task.complete(chunk, results=[failure] * len(chunk.candidates)).accepted
    task.drain()
    view = svc.get_batch(world.admin, study.study_id, batch.batch_id)
    failed = [r for r in view.results if r.failed]
    assert [r.evaluation for r in failed] == list(range(1, len(chunk.candidates) + 1))
    assert failed[0].costs == [1e6, 1e6] and "engine failed" in failed[0].error
    assert view.results[-1].costs == costs_of(vectors[-1])
    assert svc.get_study(world.admin, study.study_id).state == "OPEN"
    assert svc.study_results(world.admin, study.study_id)["failed_count"] == len(failed)


def test_a_chunk_that_never_reports_fails_its_candidates_after_three_deliveries(setup, clock):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1, on_failure="penalty")
    batch = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1)),
                             "batch-key-0001")
    (task,) = start_tasks(world)
    for delivery in (1, 2, 3):
        chunk = task.lease_chunk().chunk
        assert chunk.lease_number == delivery
        clock.advance(40)
        svc.reconcile()
    view = svc.get_batch(world.admin, study.study_id, batch.batch_id)
    assert view.state == "DONE" and view.results[0].failed
    assert "3 deliveries" in view.results[0].error
    assert task.lease_chunk().chunk is None


def test_the_fail_policy_ends_the_study_and_its_pool(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=2, on_failure="fail")
    batch = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(8)),
                             "batch-key-0001")
    tasks = start_tasks(world)
    chunk = tasks[0].lease_chunk().chunk
    results = [{"costs": None, "steps": 0, "error": "boom"}] * len(chunk.candidates)
    assert tasks[0].complete(chunk, results=results).accepted
    view = svc.get_study(world.admin, study.study_id)
    assert view.state == "FAILED" and view.failure["error"] == "boom"
    assert svc.get_batch(world.admin, study.study_id, batch.batch_id).state == "FAILED"
    assert all(t.lease_chunk().done for t in tasks)
    with svc.tx() as s:
        jobs = s.query(Job).filter(Job.study_id == study.study_id).all()
        assert all(j.cancel_requested or j.state == "CANCELLED" for j in jobs)
    with pytest.raises(PlatformError) as refused:
        svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1)),
                         "batch-key-0002")
    assert refused.value.code == ErrorCode.INVALID_TRANSITION


def test_an_evaluator_that_cannot_run_fails_the_study(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1)
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1)),
                     "batch-key-0001")
    (task,) = start_tasks(world)
    svc.agent_complete(world.target_id, task.lease.attempt_token, AttemptResult(
        attempt_id=task.attempt_id, fencing_token=task.lease.fencing_token,
        outcome=AttemptOutcome.FAILED, failure_class=FailureClass.INVALID_INPUT,
        diagnostics={"message": "the env config is invalid"}))
    view = svc.get_study(world.admin, study.study_id)
    assert view.state == "FAILED" and view.failure["message"] == "the env config is invalid"


# -- the pool ------------------------------------------------------------------------------


def test_idle_tasks_retire_and_a_new_batch_starts_a_new_one(setup, clock):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1,
                   parameters={"env_config": env_config(), "idle_timeout_seconds": 60})
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1)),
                     "batch-key-0001")
    (task,) = start_tasks(world)
    task.drain()
    assert not task.lease_chunk().done  # still within the idle timeout: wait for work
    clock.advance(61)
    assert task.lease_chunk().done
    assert svc.get_study(world.admin, study.study_id).pool[0]["retired"]
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1, 9)),
                     "batch-key-0002")
    pool = svc.get_study(world.admin, study.study_id).pool
    assert len(pool) == 2 and not pool[1]["retired"]


def test_the_reconciler_restarts_a_pool_that_lost_its_tasks(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1)
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(1)),
                     "batch-key-0001")
    (job_id,) = [p["job_id"] for p in svc.get_study(world.admin, study.study_id).pool]
    svc.cancel_job(world.admin, job_id)  # someone cancels the only task before it starts
    assert svc.reconcile()["pools_restarted"] == 1
    pool = svc.get_study(world.admin, study.study_id).pool
    assert [p["state"] for p in pool] == ["CANCELLED", "ACCEPTED"]


def test_cancelling_a_study_stops_its_pool(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=2)
    batch = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(6)),
                             "batch-key-0001")
    tasks = start_tasks(world)
    tasks[0].lease_chunk()
    view = svc.cancel_study(world.admin, study.study_id)
    assert view.state == "CANCELLED"
    assert svc.get_batch(world.admin, study.study_id, batch.batch_id).state == "CANCELLED"
    assert all(p["state"] == "CANCELLED" or j.cancel_requested
               for p in view.pool for j in [world.services.get_job(world.admin, p["job_id"])])
    response = world.poll(free_slots=0, caps=POOL_CAPS, held=[t.attempt_id for t in tasks])
    assert set(response.cancel_attempts) == {t.attempt_id for t in tasks}


# -- requests, budgets and isolation ----------------------------------------------------


def test_batches_are_idempotent_validated_and_budgeted(setup):
    world, revision = setup
    svc = world.services
    study = create(world, revision, max_evaluations=10)
    assert create(world, revision, max_evaluations=10).study_id == study.study_id  # a retry
    with pytest.raises(PlatformError) as conflict:
        create(world, revision, max_evaluations=11)
    assert conflict.value.code == ErrorCode.CONFLICT

    vectors = candidates(4)
    first = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=vectors),
                             "batch-key-0001")
    again = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=vectors),
                             "batch-key-0001")
    assert again.batch_id == first.batch_id
    with pytest.raises(PlatformError) as changed:
        svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=vectors[:2]),
                         "batch-key-0001")
    assert changed.value.code == ErrorCode.CONFLICT
    for bad in ([[3.0, 3.0]], [[5.0, 3.0, 1.0]], [[3.0, 3.0, float("nan")]]):
        with pytest.raises(PlatformError) as invalid:
            svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=bad),
                             "batch-key-0002")
        assert invalid.value.code == ErrorCode.VALIDATION_ERROR
    with pytest.raises(PlatformError) as budget:
        svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(7)),
                         "batch-key-0003")
    assert "allows 6 more" in budget.value.body.message
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(6)),
                     "batch-key-0004")


def test_a_study_must_name_the_revision_model_and_an_evaluator_workflow(setup):
    world, revision = setup
    config = env_config() | {"inp_path": "model/elsewhere.inp"}
    with pytest.raises(PlatformError) as missing:
        create(world, revision, parameters={"env_config": config})
    assert "not a file of revision" in missing.value.body.message
    with pytest.raises(PlatformError) as escape:
        create(world, revision, key="study-key-0002",
               parameters={"env_config": env_config() | {"inp_path": "../x.inp"}})
    assert escape.value.code in (ErrorCode.CAPABILITY_UNSUPPORTED, ErrorCode.NOT_READY)
    with pytest.raises(PlatformError) as plain:
        create(world, revision, key="study-key-0003", workflow="swmm.simulation",
               parameters={"threads": 1})
    assert "not an evaluator workflow" in plain.value.body.message
    with pytest.raises(PlatformError) as as_run:
        world.services.submit_run(world.admin, RunRequest(
            project_id=world.project_id, revision_id=revision["revision_id"],
            workflow="gym.evaluate", workflow_version="1", target_id=world.target_id,
            parameters={"env_config": env_config()}), "run-key-0001")
    assert "create a study" in as_run.value.body.message


def test_an_agent_session_study_beyond_its_budget_waits_for_a_person(setup):
    world, revision = setup
    svc = world.services
    session = svc.create_agent_session(world.admin, AgentSessionCreate(
        project_id=world.project_id, client_name="claude-code",
        budget=Budget(max_evaluations=100)))
    agent = svc.session_principal(session["agent_session_token"])
    within = create(world, revision, key="study-key-0001", principal=agent, max_evaluations=60)
    assert within.state == "OPEN"
    beyond = create(world, revision, key="study-key-0002", principal=agent, max_evaluations=60)
    assert beyond.state == "PENDING_APPROVAL" and beyond.approval_id
    batch = svc.submit_batch(agent, beyond.study_id, BatchSubmit(candidates=candidates(2)),
                             "batch-key-0001")
    assert batch.state == "QUEUED" and "approval" in batch.next_action
    assert svc.get_study(world.admin, beyond.study_id).pool == []
    with pytest.raises(PlatformError) as self_approval:
        svc.resolve_approval(agent, beyond.approval_id, True)
    assert self_approval.value.code == ErrorCode.FORBIDDEN
    approved = svc.resolve_approval(world.admin, beyond.approval_id, True)
    assert approved["study_state"] == "OPEN"
    assert len(svc.get_study(world.admin, beyond.study_id).pool) == 2  # one task per chunk

    rejected = create(world, revision, key="study-key-0003", principal=agent, max_evaluations=50)
    svc.resolve_approval(world.admin, rejected.approval_id, False, "too big")
    assert svc.get_study(world.admin, rejected.study_id).state == "REJECTED"
    actor = svc.study_manifest(world.admin, within.study_id)["actor"]
    assert actor["agent_session_id"] == session["agent_session_id"]


def test_other_organizations_cannot_see_or_drive_a_study(setup, artifact_dir):
    world, revision = setup
    study = create(world, revision)
    other = make_world(world.services, artifact_dir / "other", org_name="Utility B",
                       admin="bob")
    for call in (lambda: other.services.get_study(other.admin, study.study_id),
                 lambda: other.services.submit_batch(other.admin, study.study_id,
                                                     BatchSubmit(candidates=candidates(1)),
                                                     "batch-key-0001"),
                 lambda: other.services.study_results(other.admin, study.study_id),
                 lambda: other.services.cancel_study(other.admin, study.study_id)):
        with pytest.raises(PlatformError) as hidden:
            call()
        assert hidden.value.code == ErrorCode.NOT_FOUND


# -- the agent's runner gateway and the HTTP routes ------------------------------------


class PoolAdapter:
    """A scheduler that records what it was asked to start."""

    name = "local-pool"

    def __init__(self):
        self.submitted = {}

    def capabilities(self):
        return POOL_CAPS

    def free_slots(self):
        return 2

    def submit(self, lease, store_uri, env=None):
        self.submitted[lease.attempt_id] = env
        return lease.attempt_id

    def inspect(self, handle):
        from hydrocouple_platform.agent.adapters import Observation
        from hydrocouple_platform.contracts.states import AttemptState

        return Observation(state=AttemptState.RUNNING)

    def cancel(self, handle):
        pass

    def cleanup(self, handle):
        pass


@pytest.mark.parametrize("egress", ["metrics-only", "summaries"])
def test_runners_lease_and_report_only_through_their_agent(setup, egress):
    import httpx

    from hydrocouple_platform.agent import Agent
    from hydrocouple_platform.contracts.jobs import EgressPolicy
    from world import DirectAgentClient

    world, revision = setup
    svc = world.services
    study = create(world, revision, pool_size=1, on_failure="penalty")
    batch = svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(2)),
                             "batch-key-0001")
    svc.dispatch()
    adapter = PoolAdapter()
    agent = Agent(DirectAgentClient(svc, world.target_id), adapter, world.store,
                  world.store_uri, EgressPolicy(egress))
    try:
        agent.run_once()
        ((attempt_id, env),) = adapter.submitted.items()
        assert env["HCP_GATEWAY_URL"].startswith("http://127.0.0.1:")
        gateway = httpx.Client(base_url=env["HCP_GATEWAY_URL"],
                               headers={"Authorization": f"Bearer {env['HCP_RUNNER_TOKEN']}"})
        assert httpx.post(env["HCP_GATEWAY_URL"] + "/v1/evaluations/lease", json={},
                          headers={"Authorization": "Bearer hcpr_wrong"}).status_code == 401

        lease = agent.held[attempt_id].lease
        prefix = f"{lease.spec.outputs.storage_prefix}{attempt_id}/evaluations/"
        while (chunk := gateway.post("/v1/evaluations/lease", json={}).json()["chunk"]):
            local = world.root / f"{chunk['chunk_id']}.jsonl"
            local.write_text("{}\n")
            key = prefix + local.name
            body = {"chunk_id": chunk["chunk_id"], "lease_number": chunk["lease_number"],
                    "results": [{"costs": None, "error": "Traceback: node J7 surcharged"}
                                for _ in chunk["candidates"]],
                    "object": {"key": key, "sha256": sha256_file(local),
                               "size": local.stat().st_size}}
            missing = gateway.post("/v1/evaluations/complete", json=body)
            assert missing.status_code == 422  # the records object is not in storage yet
            world.store.put_file(key, local)
            outside = dict(body, object=dict(body["object"], key="runs/elsewhere.jsonl"))
            assert gateway.post("/v1/evaluations/complete", json=outside).status_code == 422
            assert gateway.post("/v1/evaluations/complete", json=body).json()["accepted"]
        view = svc.get_batch(world.admin, study.study_id, batch.batch_id)
        errors = {r.error for r in view.results}
        if egress == "metrics-only":
            assert errors == {"evaluation failed (details withheld by this site's egress policy)"}
        else:
            assert errors == {"Traceback: node J7 surcharged"}
    finally:
        agent.close()


def test_study_routes_over_http(setup, server, make_token):
    from hydrocouple_platform.client import PlatformClient

    world, revision = setup
    with PlatformClient(server, make_token("alice")) as client:
        body = StudyCreate(
            project_id=world.project_id, revision_id=revision["revision_id"],
            target_id=world.target_id, parameters={"env_config": env_config()},
            dimensions=DIMS, objectives=OBJECTIVES, max_evaluations=10)
        study = client.create_study(body, "http-study-0001")
        assert client.create_study(body, "http-study-0001")["study_id"] == study["study_id"]
        batch = client.submit_batch(study["study_id"], candidates(3), "http-batch-0001")
        assert client.batch(study["study_id"], batch["batch_id"])["state"] == "QUEUED"
        assert client.study_evaluations(study["study_id"])["evaluations"] == []
        assert client.study_results(study["study_id"])["pareto"] == []
        assert client.study_manifest(study["study_id"])["batches"][0]["size"] == 3
        cancelled = client.cancel_study(study["study_id"])
        assert cancelled["state"] == "CANCELLED"
        with pytest.raises(PlatformError) as refused:
            client.submit_batch(study["study_id"], candidates(1), "http-batch-0002")
        assert refused.value.code == ErrorCode.INVALID_TRANSITION


def test_the_migrations_build_exactly_the_models(database_url):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import create_engine

    from hydrocouple_platform.db.models import Base

    engine = create_engine(database_url)
    try:
        with engine.connect() as conn:
            assert compare_metadata(MigrationContext.configure(conn), Base.metadata) == []
    finally:
        engine.dispose()
