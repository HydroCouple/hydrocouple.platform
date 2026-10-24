"""Failure classification, fencing, lease expiry, deadlines and cancellation races."""

from __future__ import annotations

import pytest

from hydrocouple_platform.contracts.agent import (
    AttemptOutcome,
    AttemptResult,
    Heartbeat,
)
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.jobs import FailureClass
from hydrocouple_platform.contracts.states import AttemptState
from world import fake_outputs, make_world, ready_revision

pytestmark = pytest.mark.postgres


@pytest.fixture
def world(services, artifact_dir):
    return make_world(services, artifact_dir)


def start(world, retry_budget=2, key="key-0001"):
    revision = ready_revision(world)
    accepted = world.services.submit_run(
        world.admin, world.run_request(revision["revision_id"], retry_budget=retry_budget), key)
    world.services.dispatch()
    return accepted


def lease(world):
    leases = world.poll().leases
    assert len(leases) == 1
    return leases[0]


def fail(world, lease_, cls, outcome=AttemptOutcome.FAILED):
    return world.services.agent_complete(world.target_id, lease_.attempt_token, AttemptResult(
        attempt_id=lease_.attempt_id, fencing_token=lease_.fencing_token, outcome=outcome,
        failure_class=cls, exit_code=12))


def state(world, job_id):
    return world.services.get_job(world.admin, job_id)


def test_deterministic_failures_never_retry(world):
    job = start(world).job_id
    ack = fail(world, lease(world), FailureClass.INVALID_INPUT)
    assert ack.job_state == "FAILED" and not ack.retry_scheduled
    assert state(world, job).failure["class"] == "invalid_input"


def test_engine_crash_retries_once_with_a_new_fencing_token(world):
    job = start(world, retry_budget=5).job_id
    first = lease(world)
    assert fail(world, first, FailureClass.ENGINE_CRASH).retry_scheduled
    second = lease(world)
    assert second.fencing_token == first.fencing_token + 1
    assert second.attempt_id != first.attempt_id
    ack = fail(world, second, FailureClass.ENGINE_CRASH)
    assert ack.job_state == "FAILED"
    assert [a["number"] for a in state(world, job).attempts] == [1, 2]


def test_retry_budget_bounds_interruptions(world):
    start(world, retry_budget=1)
    assert fail(world, lease(world), FailureClass.INTERRUPTED).retry_scheduled
    assert fail(world, lease(world), FailureClass.INTERRUPTED).job_state == "FAILED"


def test_a_superseded_attempt_cannot_report_or_publish(world, clock):
    job = start(world).job_id
    old = lease(world)
    # The agent goes silent; past the abandon deadline the attempt is lost and retried.
    clock.advance(world.services.settings.lease_seconds + 1)
    assert world.services.reconcile()["unreachable"] == 1
    assert state(world, job).agent_unreachable
    clock.advance(world.services.settings.abandon_after_seconds + 1)
    assert world.services.reconcile()["lost"] == 1
    new = lease(world)
    assert new.fencing_token == 2
    # The old attempt resurfaces: every report is refused, and it is told to stop.
    with pytest.raises(PlatformError) as err:
        world.services.agent_heartbeat(world.target_id, old.attempt_token, Heartbeat(
            attempt_id=old.attempt_id, fencing_token=old.fencing_token,
            state=AttemptState.RUNNING))
    assert err.value.code in (ErrorCode.STALE_ATTEMPT, ErrorCode.UNAUTHENTICATED)
    with pytest.raises(PlatformError):
        world.services.agent_complete(world.target_id, old.attempt_token, fake_outputs(world, old))
    assert old.attempt_id in world.poll(free_slots=0, held=[old.attempt_id]).cancel_attempts
    # The current attempt publishes normally.
    ack = world.services.agent_complete(world.target_id, new.attempt_token,
                                        fake_outputs(world, new))
    assert ack.canonical and state(world, job).state == "SUCCEEDED"


def test_attempt_tokens_are_bound_to_their_attempt(world):
    start(world)
    first = lease(world)
    with pytest.raises(PlatformError) as err:
        world.services.agent_heartbeat(world.target_id, first.attempt_token, Heartbeat(
            attempt_id=first.attempt_id, fencing_token=first.fencing_token + 1,
            state=AttemptState.RUNNING))
    assert err.value.code == ErrorCode.STALE_ATTEMPT


def test_heartbeats_keep_a_slow_attempt_alive(world, clock):
    job = start(world).job_id
    held = lease(world)
    token = held.attempt_token
    for _ in range(5):
        clock.advance(world.services.settings.lease_seconds * 0.8)
        token = world.services.agent_heartbeat(world.target_id, token, Heartbeat(
            attempt_id=held.attempt_id, fencing_token=held.fencing_token,
            state=AttemptState.RUNNING)).attempt_token
        assert not any(world.services.reconcile().values())
    assert state(world, job).state == "RUNNING"


def test_capacity_shortage_is_visible_until_the_queue_deadline(world, clock):
    job = start(world).job_id
    assert state(world, job).state == "LEASABLE"
    clock.advance(24 * 3600 + 1)
    assert world.services.reconcile()["queue_timeouts"] == 1
    view = state(world, job)
    assert view.state == "TIMED_OUT" and view.failure["class"] == "capacity"


def test_cancel_before_a_lease_is_immediate(world):
    job = start(world).job_id
    assert world.services.cancel_job(world.admin, job)["state"] == "CANCELLED"
    assert world.poll().leases == []


def test_cancel_while_running_reaches_the_agent(world):
    job = start(world).job_id
    held = lease(world)
    assert world.services.cancel_job(world.admin, job)["cancel_requested"]
    ack = world.services.agent_heartbeat(world.target_id, held.attempt_token, Heartbeat(
        attempt_id=held.attempt_id, fencing_token=held.fencing_token, state=AttemptState.RUNNING))
    assert ack.cancel_requested
    assert held.attempt_id in world.poll(free_slots=0, held=[held.attempt_id]).cancel_attempts
    done = world.services.agent_complete(world.target_id, ack.attempt_token, AttemptResult(
        attempt_id=held.attempt_id, fencing_token=held.fencing_token,
        outcome=AttemptOutcome.CANCELLED, exit_code=14))
    assert done.job_state == "CANCELLED"


def test_completion_that_races_a_cancel_keeps_the_observed_outcome(world):
    job = start(world).job_id
    held = lease(world)
    world.services.cancel_job(world.admin, job)
    ack = world.services.agent_complete(world.target_id, held.attempt_token,
                                        fake_outputs(world, held))
    assert ack.job_state == "SUCCEEDED" and ack.canonical


def test_missing_artifacts_prevent_success(world):
    job = start(world, retry_budget=0).job_id
    held = lease(world)
    ack = world.services.agent_complete(world.target_id, held.attempt_token,
                                        fake_outputs(world, held, drop="model.out"))
    assert ack.job_state == "FAILED"
    failure = state(world, job).failure
    assert failure["class"] == "unknown"  # the runner contradicted the workflow
    assert any("model.out" in p for p in failure["diagnostics"]["manifest_problems"])


def test_outbox_survives_a_dispatcher_restart(services, artifact_dir, settings, clock):
    from hydrocouple_platform.db import make_engine, make_sessionmaker
    from hydrocouple_platform.services import PlatformServices

    world = make_world(services, artifact_dir)
    revision = ready_revision(world)
    accepted = services.submit_run(world.admin, world.run_request(revision["revision_id"]),
                                   "key-restart")
    # A new process (fresh engine, no shared memory) picks up the accepted work.
    engine = make_engine(settings.database_url)
    fresh = PlatformServices(make_sessionmaker(engine), settings, clock)
    assert fresh.dispatch() == 1
    assert fresh.dispatch() == 0
    assert fresh.get_job(world.admin, accepted.job_id).state == "LEASABLE"
    engine.dispose()


# -- regressions from the independent review -----------------------------------------


def test_a_job_that_does_not_fit_stays_queued_and_polls_keep_working(world):
    from world import local_capabilities

    job = start(world).job_id
    small = local_capabilities(scratch_gib=1)  # e.g. the disk filled up
    response = world.poll(caps=small)
    assert response.leases == []
    view = state(world, job)
    assert view.state == "LEASABLE" and "scratch" in view.progress["message"]
    assert len(world.poll().leases) == 1  # disk freed: it runs


def test_a_retry_gets_a_fresh_queue_deadline(world, clock):
    job = start(world).job_id
    clock.advance(20 * 3600)  # waited most of its 24 h queue deadline
    first = lease(world)
    clock.advance(5 * 3600)
    assert fail(world, first, FailureClass.INTERRUPTED).retry_scheduled
    assert world.services.reconcile()["queue_timeouts"] == 0
    assert state(world, job).state == "LEASABLE"


def test_a_stop_nobody_asked_for_is_retried_not_cancelled(world):
    job = start(world).job_id
    ack = fail(world, lease(world), None, outcome=AttemptOutcome.CANCELLED)
    assert ack.retry_scheduled and state(world, job).failure["class"] == "interrupted"


def test_an_expired_attempt_token_still_reports_while_its_attempt_holds_the_job(
        world, services):
    import time as _time

    from hydrocouple_platform.services.auth import AttemptTokens

    start(world)
    held = lease(world)
    expired = AttemptTokens("s" * 48, 60)
    expired._lifetime = -1  # already expired when issued
    token = expired.issue(target_id=world.target_id, job_id=held.job_id,
                          attempt_id=held.attempt_id, fence=held.fencing_token)
    _time.sleep(0.01)
    ack = services.agent_heartbeat(world.target_id, token, Heartbeat(
        attempt_id=held.attempt_id, fencing_token=held.fencing_token,
        state=AttemptState.RUNNING))
    assert ack.attempt_token != token  # renewed
    published = services.agent_complete(world.target_id, token, fake_outputs(world, held))
    assert published.canonical


def test_the_server_enforces_the_execution_timeout(world, clock):
    job = start(world).job_id
    held = lease(world)
    token = held.attempt_token
    limit = held.spec.limits.execution_timeout_seconds + world.services.settings.lease_seconds
    for _ in range(int(limit // 25) + 2):  # heartbeats keep the lease alive throughout
        clock.advance(25)
        try:
            token = world.services.agent_heartbeat(world.target_id, token, Heartbeat(
                attempt_id=held.attempt_id, fencing_token=held.fencing_token,
                state=AttemptState.RUNNING)).attempt_token
        except PlatformError:
            break
        world.services.reconcile()
    assert state(world, job).state == "TIMED_OUT"


def test_cancel_notices_cover_only_attempts_the_agent_holds(world, clock):
    start(world, retry_budget=3)
    old = lease(world)
    clock.advance(world.services.settings.lease_seconds + world.services
                  .settings.abandon_after_seconds + 2)
    world.services.reconcile()
    assert world.poll(free_slots=0).cancel_attempts == []  # nothing held: nothing to stop
    stop = world.poll(free_slots=0, held=[old.attempt_id, "att_unknown"]).cancel_attempts
    assert stop == sorted([old.attempt_id, "att_unknown"])


def test_a_lease_whose_response_was_lost_is_offered_again(world):
    start(world)
    first = world.poll().leases[0]  # the agent never receives this response
    again = world.poll().leases
    assert [lease_.attempt_id for lease_ in again] == [first.attempt_id]
    assert world.poll(held=[first.attempt_id]).leases == []  # once held, not re-offered


def test_a_manifest_listing_an_artifact_twice_fails_cleanly(world):
    job = start(world, retry_budget=0).job_id
    held = lease(world)
    result = fake_outputs(world, held)
    manifest = dict(result.attempt_manifest)
    manifest["artifacts"] = manifest["artifacts"] + manifest["artifacts"][:1]
    from hydrocouple_platform.contracts.manifests import sha256_of

    bad = result.model_copy(update={"attempt_manifest": manifest,
                                    "attempt_manifest_sha256": sha256_of(manifest)})
    ack = world.services.agent_complete(world.target_id, held.attempt_token, bad)
    assert ack.job_state == "FAILED" and state(world, job).failure["class"] == "unknown"


def test_default_and_explicit_retry_budget_are_the_same_request(world):
    revision = ready_revision(world)
    a = world.services.submit_run(world.admin, world.run_request(revision["revision_id"]),
                                  "key-default-1")
    b = world.services.submit_run(
        world.admin, world.run_request(revision["revision_id"], retry_budget=2), "key-default-1")
    assert a.run_id == b.run_id


def test_concurrent_first_sign_ins_create_one_user(services):
    from concurrent.futures import ThreadPoolExecutor

    from conftest import ISSUER
    from hydrocouple_platform.services import VerifiedClaims

    claims = VerifiedClaims(ISSUER, "first-timer", None)
    with ThreadPoolExecutor(8) as pool:
        ids = {p.user_id for p in pool.map(lambda _: services.principal_for(claims), range(16))}
    assert len(ids) == 1


def test_a_revision_cannot_name_another_projects_session(services, artifact_dir):
    from conftest import DATA
    from hydrocouple_platform.contracts.requests import RevisionCreate
    from hydrocouple_platform.packaging import package_swmm_model

    a = make_world(services, artifact_dir / "a", org_name="Utility A", admin="alice")
    b = make_world(services, artifact_dir / "b", org_name="Utility B", admin="bob")
    foreign = b.services.create_session(b.admin, b.project_id, "bob's session")
    package = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "pkg")
    with pytest.raises(PlatformError) as err:
        a.services.create_revision(a.admin, RevisionCreate(
            project_id=a.project_id, target_id=a.target_id,
            session_id=foreign["platform_session_id"], manifest=package.manifest))
    assert err.value.code == ErrorCode.NOT_FOUND
