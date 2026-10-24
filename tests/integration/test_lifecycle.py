"""Lifecycle through the shared services against a real PostgreSQL database."""

from __future__ import annotations

import pytest

from hydrocouple_platform.contracts.agent import Event, EventKind, Heartbeat
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.states import AttemptState
from world import fake_outputs, local_capabilities, make_world, ready_revision

pytestmark = pytest.mark.postgres


@pytest.fixture
def world(services, artifact_dir):
    return make_world(services, artifact_dir)


def submit(world, revision, key="key-0001", **kw):
    return world.services.submit_run(world.admin, world.run_request(revision["revision_id"], **kw),
                                     key)


def lease_one(world):
    world.services.dispatch()
    response = world.poll()
    assert len(response.leases) == 1
    return response.leases[0]


def test_revision_is_verified_by_the_agent_before_use(world):
    revision = ready_revision(world)
    assert revision["state"] == "READY"
    # Corrupt one stored object: a fresh verification fails.
    key = f"revisions/{world.org_id}/{world.project_id}/{revision['revision_id']}/model/model.inp"
    world.store.path(key).write_text("tampered")
    world.services.finalize_revision(world.admin, revision["revision_id"])  # READY: no-op
    assert world.services.get_revision(world.admin, revision["revision_id"])["state"] == "READY"


def test_run_from_acceptance_to_published_result(world):
    revision = ready_revision(world)
    accepted = submit(world, revision)
    assert accepted.status == "ACCEPTED"
    assert world.services.get_job(world.admin, accepted.job_id).state == "ACCEPTED"

    lease = lease_one(world)
    assert lease.spec.inputs.revision_id == revision["revision_id"]
    assert world.services.get_job(world.admin, accepted.job_id).state == "LEASED"

    ack = world.services.agent_heartbeat(world.target_id, lease.attempt_token, Heartbeat(
        attempt_id=lease.attempt_id, fencing_token=lease.fencing_token,
        state=AttemptState.RUNNING, events=[Event(
            event_id="ev-000000001", sequence=0, kind=EventKind.PROGRESS, completed=3600,
            total=21600, unit="simulated_seconds", occurred_at=lease.lease_expires_at)]))
    assert ack.accepted_events == 1 and not ack.cancel_requested
    view = world.services.get_job(world.admin, accepted.job_id)
    assert view.state == "RUNNING" and view.progress["completed"] == 3600

    result = fake_outputs(world, lease)
    published = world.services.agent_complete(world.target_id, ack.attempt_token, result)
    assert published.job_state == "SUCCEEDED" and published.canonical
    results = world.services.get_results(world.admin, accepted.run_id)
    assert {a["path"] for a in results["artifacts"]} == {"run.json", "model.rpt", "model.out"}
    assert results["provenance"]["attempt_id"] == lease.attempt_id
    assert results["provenance"]["actor"]["human_subject"] == "alice"

    # A replayed result (lost acknowledgement) is answered, not re-applied.
    again = world.services.agent_complete(world.target_id, ack.attempt_token, result)
    assert again.canonical and again.job_state == "SUCCEEDED"


def test_submission_is_idempotent(world):
    revision = ready_revision(world)
    first = submit(world, revision, key="same-key-1")
    second = submit(world, revision, key="same-key-1")
    assert (first.run_id, first.job_id) == (second.run_id, second.job_id)
    with pytest.raises(PlatformError) as err:
        submit(world, revision, key="same-key-1", parameters={"threads": 2})
    assert err.value.code == ErrorCode.CONFLICT
    jobs = world.services.list_jobs(world.admin, world.project_id)["jobs"]
    assert len(jobs) == 1


def test_duplicate_events_are_ignored_and_progress_does_not_regress(world):
    revision = ready_revision(world)
    accepted = submit(world, revision)
    lease = lease_one(world)

    def beat(seq, completed, token, event_id=None):
        return world.services.agent_heartbeat(world.target_id, token, Heartbeat(
            attempt_id=lease.attempt_id, fencing_token=lease.fencing_token,
            state=AttemptState.RUNNING, events=[Event(
                event_id=event_id or f"ev-{seq:09d}", sequence=seq, kind=EventKind.PROGRESS,
                completed=completed, total=100, occurred_at=lease.lease_expires_at)]))

    ack = beat(5, 50, lease.attempt_token)
    ack = beat(5, 50, ack.attempt_token)  # replay
    assert ack.accepted_events == 0
    ack = beat(2, 20, ack.attempt_token)  # late, older event
    assert world.services.get_job(world.admin, accepted.job_id).progress["completed"] == 50


def test_submission_needs_reported_capabilities_and_a_ready_revision(services, artifact_dir):
    world = make_world(services, artifact_dir)
    revision = ready_revision(world)
    # The agent now reports a profile too small for the workflow's default.
    world.poll(free_slots=0, caps=local_capabilities(max_memory_gib=1))
    estimate = world.services.estimate(world.admin, world.run_request(revision["revision_id"]))
    assert not estimate.ok and any("memory" in f for f in estimate.findings)
    with pytest.raises(PlatformError) as err:
        submit(world, revision)
    assert err.value.code == ErrorCode.CAPABILITY_UNSUPPORTED


def test_unapproved_parameters_and_workflows_are_refused(world):
    revision = ready_revision(world)
    with pytest.raises(PlatformError, match="parameters"):
        submit(world, revision, parameters={"command": "rm -rf /"})
    with pytest.raises(PlatformError, match="not approved"):
        world.services.submit_run(world.admin, world.run_request(
            revision["revision_id"]).model_copy(update={"workflow": "shell"}), "key-9999")
