"""The agent's own rules, driven by a scripted scheduler against the real services."""

from __future__ import annotations

import threading

import pytest

from hydrocouple_platform.agent import Agent
from hydrocouple_platform.agent.adapters import Observation
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.jobs import EgressPolicy
from hydrocouple_platform.contracts.states import AttemptState
from hydrocouple_platform.runner import EXIT_CANCELLED
from world import DirectAgentClient, fake_outputs, local_capabilities, make_world, ready_revision

pytestmark = pytest.mark.postgres


class ScriptedAdapter:
    """A scheduler whose attempts finish however the test says."""

    name = "local-pool"

    def __init__(self, world, *, fail_submit=False):
        self.world, self.fail_submit = world, fail_submit
        self.leases, self.outcome, self.cancelled = {}, {}, set()

    def capabilities(self):
        return local_capabilities()

    def free_slots(self):
        return 2

    def submit(self, lease, store_uri):
        if self.fail_submit:
            raise OSError("no space left on device")
        self.leases[lease.attempt_id] = lease
        return lease.attempt_id

    def finish(self, attempt_id, **outcome):
        self.outcome[attempt_id] = outcome

    def inspect(self, handle):
        outcome = self.outcome.get(handle)
        if outcome is None:
            return Observation(state=AttemptState.RUNNING)
        return Observation(state=AttemptState.FINALIZING, finished=True, **outcome)

    def cancel(self, handle):
        self.cancelled.add(handle)

    def cleanup(self, handle):
        pass


@pytest.fixture
def world(services, artifact_dir):
    return make_world(services, artifact_dir)


def agent_for(world, adapter):
    return Agent(DirectAgentClient(world.services, world.target_id), adapter, world.store,
                 world.store_uri, EgressPolicy.SUMMARIES, instance="agent-1")


def submit(world, key="key-agent-1", retry_budget=2):
    revision = ready_revision(world)
    accepted = world.services.submit_run(
        world.admin, world.run_request(revision["revision_id"], retry_budget=retry_budget), key)
    world.services.dispatch()
    return accepted.job_id


def test_the_agent_verifies_outputs_and_publishes(world):
    job = submit(world)
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    agent.run_once()
    (attempt_id, lease), = adapter.leases.items()
    fake_outputs(world, lease)
    adapter.finish(attempt_id, exit_code=0)
    agent.run_once()
    assert world.services.get_job(world.admin, job).state == "SUCCEEDED"
    assert agent.held == {}


def test_tampered_outputs_are_not_published(world):
    job = submit(world, retry_budget=0)
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    agent.run_once()
    (attempt_id, lease), = adapter.leases.items()
    fake_outputs(world, lease)
    key = f"{lease.spec.outputs.storage_prefix}{attempt_id}/model.out"
    world.store.path(key).write_bytes(b"changed after the manifest was written")
    adapter.finish(attempt_id, exit_code=0)
    agent.run_once()
    view = world.services.get_job(world.admin, job)
    assert view.state == "FAILED" and view.failure["class"] == "transfer"


def test_a_failed_local_start_is_reported_and_retried_at_once(world):
    job = submit(world)
    agent = agent_for(world, ScriptedAdapter(world, fail_submit=True))
    agent.run_once()
    view = world.services.get_job(world.admin, job)
    assert view.state == "LEASABLE" and view.failure["class"] == "capacity"
    assert "no space left" in view.failure["diagnostics"]["message"]


def test_a_stop_the_agent_did_not_send_is_an_interruption(world):
    job = submit(world)
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    agent.run_once()
    (attempt_id, _), = adapter.leases.items()
    adapter.finish(attempt_id, exit_code=EXIT_CANCELLED)  # e.g. SIGTERM from an eviction
    agent.run_once()
    view = world.services.get_job(world.admin, job)
    assert view.state == "LEASABLE" and view.failure["class"] == "interrupted"


def test_a_user_cancel_reaches_the_scheduler(world):
    job = submit(world)
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    agent.run_once()
    (attempt_id, _), = adapter.leases.items()
    world.services.cancel_job(world.admin, job)
    agent.run_once()
    assert attempt_id in adapter.cancelled
    adapter.finish(attempt_id, exit_code=EXIT_CANCELLED, cancelled_by_agent=True)
    agent.run_once()
    assert world.services.get_job(world.admin, job).state == "CANCELLED"


def test_a_revoked_target_drains_its_work_and_stops(world):
    job = submit(world)
    adapter = ScriptedAdapter(world)
    agent = agent_for(world, adapter)
    agent.run_once()
    (attempt_id, lease), = adapter.leases.items()
    world.services.revoke_target(world.admin, world.target_id)
    with pytest.raises(PlatformError) as err:
        world.services.agent_target(world.credential)
    assert err.value.code == ErrorCode.TARGET_REVOKED
    assert world.services.agent_target(world.credential, allow_revoked=True).id == world.target_id
    agent.run_once()
    assert agent.draining
    fake_outputs(world, lease)
    adapter.finish(attempt_id, exit_code=0)
    stop = threading.Event()
    agent.run_forever(stop, wait_seconds=0)  # returns once the held attempt has published
    assert world.services.get_job(world.admin, job).state == "SUCCEEDED"
