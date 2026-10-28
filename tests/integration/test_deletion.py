"""Deleting runs and studies (plan §12): asynchronous, confirmed, and honest when blocked.

The agent here is the real agent with a scripted scheduler and the file store, so the
erasure really deletes objects. ``tests/k8s`` deletes on a cluster with S3.
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select
from test_agent import ScriptedAdapter

from hydrocouple_platform.agent import Agent
from hydrocouple_platform.contracts.agent import AttemptOutcome, AttemptResult
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.jobs import EgressPolicy
from hydrocouple_platform.db.models import Artifact, AuditRecord, Run
from world import DirectAgentClient, fake_outputs, make_world, ready_revision

pytestmark = pytest.mark.postgres


@pytest.fixture
def world(services, artifact_dir):
    return make_world(services, artifact_dir)


def agent_for(world, adapter):
    return Agent(DirectAgentClient(world.services, world.target_id), adapter, world.store,
                 world.store_uri, EgressPolicy.SUMMARIES, instance="agent-1")


def published_run(world, adapter, agent, key="run-key-0001"):
    revision = ready_revision(world)
    accepted = world.services.submit_run(world.admin, world.run_request(
        revision["revision_id"]), key)
    world.services.dispatch()
    agent.run_once()
    lease = adapter.leases[max(adapter.leases)]
    fake_outputs(world, lease)
    adapter.finish(lease.attempt_id, exit_code=0)
    agent.run_once()
    assert world.services.get_job(world.admin, accepted.job_id).state == "SUCCEEDED"
    return accepted, revision, lease


def run_files(world, run_id) -> list:
    root = world.store.root / "runs" / world.org_id / world.project_id / run_id
    return [p for p in root.rglob("*") if p.is_file()] if root.exists() else []


def test_a_published_run_is_erased_in_two_passes_and_its_revision_kept(world, clock):
    svc = world.services
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    accepted, revision, _ = published_run(world, adapter, agent)
    assert run_files(world, accepted.run_id)

    deletion = svc.delete_run(world.admin, accepted.run_id)
    assert deletion.state == "ERASING"  # nothing was running, so erasure is due at once
    assert svc.delete_run(world.admin, accepted.run_id).deletion_id == deletion.deletion_id
    for refused in (lambda: svc.get_results(world.admin, accepted.run_id),
                    lambda: svc.request_downloads(world.admin, accepted.run_id, ["model.out"])):
        with pytest.raises(PlatformError) as gone:
            refused()
        assert gone.value.code == ErrorCode.NOT_FOUND
        assert "being deleted" in gone.value.body.message

    agent.run_once()  # the agent erases the run's prefix
    view = svc.get_deletion(world.admin, deletion.deletion_id)
    assert view.state == "VERIFYING" and view.passes == 1 and view.objects_deleted >= 4
    assert run_files(world, accepted.run_id) == []
    agent.run_once()
    assert svc.get_deletion(world.admin, deletion.deletion_id).passes == 1  # not due yet

    clock.advance(svc.settings.deletion_recheck_seconds + 1)
    agent.run_once()  # the second pass finds nothing left
    view = svc.get_deletion(world.admin, deletion.deletion_id)
    assert view.state == "ERASED" and view.passes == 2 and view.completed_at is not None
    with pytest.raises(PlatformError) as gone:
        svc.get_results(world.admin, accepted.run_id)
    assert "was deleted" in gone.value.body.message
    with svc.tx() as s:
        assert s.scalar(select(func.count()).select_from(Artifact).where(
            Artifact.run_id == accepted.run_id)) == 0
        assert s.get(Run, accepted.run_id).deleted_at is not None  # kept as a tombstone
        actions = list(s.scalars(select(AuditRecord.action).where(
            AuditRecord.object_id == accepted.run_id)))
    assert {"run.delete", "run.erased"} <= set(actions)
    # The revision is a shared input: its files stay.
    assert svc.get_revision(world.admin, revision["revision_id"])["state"] == "READY"
    assert any((world.store.root / "revisions").rglob("*.inp"))


def test_erasure_waits_for_a_running_attempt_and_its_result_is_not_published(world):
    svc = world.services
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    revision = ready_revision(world)
    accepted = svc.submit_run(world.admin, world.run_request(revision["revision_id"]),
                              "run-key-0001")
    svc.dispatch()
    agent.run_once()
    (attempt_id, lease), = adapter.leases.items()

    deletion = svc.delete_run(world.admin, accepted.run_id)
    assert deletion.state == "WAITING_FOR_WRITERS"
    agent.run_once()
    assert attempt_id in adapter.cancelled  # the agent was told to stop it
    assert svc.get_deletion(world.admin, deletion.deletion_id).state == "WAITING_FOR_WRITERS"
    # It finishes anyway, successfully, before the stop lands: nothing is published.
    fake_outputs(world, lease)
    adapter.finish(attempt_id, exit_code=0)
    agent.run_once()
    job = svc.get_job(world.admin, accepted.job_id)
    assert job.state == "CANCELLED" and job.attempts[-1]["state"] == "CANCELLED"
    with svc.tx() as s:
        assert s.get(Run, accepted.run_id).canonical_attempt_id is None
    svc.reconcile()
    agent.run_once()
    assert svc.get_deletion(world.admin, deletion.deletion_id).state == "VERIFYING"
    assert run_files(world, accepted.run_id) == []


def test_an_attempt_that_never_stops_leaves_the_deletion_blocked_and_saying_why(world, clock):
    svc = world.services
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    revision = ready_revision(world)
    accepted = svc.submit_run(world.admin, world.run_request(revision["revision_id"]),
                              "run-key-0001")
    svc.dispatch()
    agent.run_once()
    (attempt_id, _), = adapter.leases.items()
    deletion = svc.delete_run(world.admin, accepted.run_id)
    clock.advance(svc.settings.deletion_writer_wait_seconds + 1)
    svc.reconcile()
    view = svc.get_deletion(world.admin, deletion.deletion_id)
    # Meanwhile the reconciler has marked the silent attempt lost, but nothing confirmed that
    # its runner stopped, so the erasure does not start.
    assert svc.get_job(world.admin, accepted.job_id).attempts[-1]["state"] == "LOST"
    assert view.state == "BLOCKED" and "erasure blocked" in view.reason
    assert accepted.job_id in view.reason and "agent last polled" in view.reason
    assert view.objects_deleted == 0  # never described as erased
    # The agent comes back: told to stop the attempt, it does, and lets go of it.
    agent.run_once()
    assert attempt_id in adapter.cancelled
    adapter.finish(attempt_id, exit_code=14, cancelled_by_agent=True)
    agent.run_once()
    agent.run_once()
    svc.reconcile()
    assert svc.get_deletion(world.admin, deletion.deletion_id).state == "ERASING"


def test_a_late_write_is_found_by_the_second_pass(world, clock, artifact_dir):
    svc = world.services
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    accepted, _, lease = published_run(world, adapter, agent)
    deletion = svc.delete_run(world.admin, accepted.run_id)
    agent.run_once()
    straggler = artifact_dir / "late.txt"
    straggler.write_text("written after the first pass")
    world.store.put_file(f"{lease.spec.outputs.storage_prefix}{lease.attempt_id}/late.txt",
                         straggler)
    clock.advance(svc.settings.deletion_recheck_seconds + 1)
    agent.run_once()
    view = svc.get_deletion(world.admin, deletion.deletion_id)
    assert view.state == "VERIFYING" and view.passes == 2  # found one, so check once more
    clock.advance(svc.settings.deletion_recheck_seconds + 1)
    agent.run_once()
    assert svc.get_deletion(world.admin, deletion.deletion_id).state == "ERASED"
    assert run_files(world, accepted.run_id) == []


def test_an_agent_session_deletes_only_runs_it_submitted(world):
    from hydrocouple_platform.contracts.requests import AgentSessionCreate

    svc = world.services
    adapter = ScriptedAdapter(world)
    accepted, _, _ = published_run(world, adapter, agent_for(world, adapter))
    session = svc.create_agent_session(world.admin, AgentSessionCreate(
        project_id=world.project_id, client_name="claude-code"))
    agent = svc.session_principal(session["agent_session_token"])
    with pytest.raises(PlatformError) as refused:
        svc.delete_run(agent, accepted.run_id)
    assert refused.value.code == ErrorCode.FORBIDDEN


def test_deleting_a_study_stops_it_and_erases_its_runs_objects(world):
    from test_studies import POOL_CAPS, Task, candidates, create

    from hydrocouple_platform.contracts.studies import BatchSubmit

    svc = world.services
    revision = ready_revision(world)
    world.poll(free_slots=0, caps=POOL_CAPS)
    study = create(world, revision, pool_size=1)
    svc.submit_batch(world.admin, study.study_id, BatchSubmit(candidates=candidates(2)),
                     "batch-key-0001")
    svc.dispatch()
    (lease,) = world.poll(free_slots=1, caps=POOL_CAPS).leases
    task = Task(world, lease)
    task.complete(task.lease_chunk().chunk)  # one chunk's records are in the store
    deletion = svc.delete_study(world.admin, study.study_id)
    assert svc.get_study(world.admin, study.study_id).state == "CANCELLED"
    assert deletion.study_id == study.study_id and deletion.state == "WAITING_FOR_WRITERS"
    svc.agent_complete(world.target_id, lease.attempt_token, AttemptResult(
        attempt_id=lease.attempt_id, fencing_token=lease.fencing_token,
        outcome=AttemptOutcome.CANCELLED, exit_code=14))
    svc.reconcile()
    agent = agent_for(world, ScriptedAdapter(world))
    agent.run_once()
    view = svc.get_deletion(world.admin, deletion.deletion_id)
    assert view.state == "VERIFYING" and view.objects_deleted >= 1
    assert run_files(world, study.run_id) == []


def test_an_agent_erases_run_prefixes_only(world):
    from hydrocouple_platform.contracts.deletion import Erasure

    reports = []

    class Recording(DirectAgentClient):
        def erasure(self, report):
            reports.append(report)

    agent = Agent(Recording(world.services, world.target_id), ScriptedAdapter(world),
                  world.store, world.store_uri, EgressPolicy.SUMMARIES)
    for prefix in ("revisions/rev_1/", "runs/org/prj/", "runs/../x/y/", "runs/a/b/c/d/"):
        agent._erase(Erasure(deletion_id="del_1", pass_number=1, prefixes=[prefix]))
    assert all(not r.ok and "not a run prefix" in r.problems[0] for r in reports[:2])
    assert all(not r.ok for r in reports)


def test_deletion_over_http(world, server, make_token):
    from hydrocouple_platform.client import PlatformClient

    adapter = ScriptedAdapter(world)
    accepted, _, _ = published_run(world, adapter, agent_for(world, adapter))
    with PlatformClient(server, make_token("alice")) as client:
        deletion = client.delete_run(accepted.run_id)
        assert deletion["state"] == "ERASING"
        assert client.deletion(deletion["deletion_id"])["run_id"] == accepted.run_id
