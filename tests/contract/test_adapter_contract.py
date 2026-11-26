"""The scheduler adapter contract (plan §5 and §16), the same cases for every adapter.

Each case runs once per harness in ``harnesses.py``: the local pool always, and Kubernetes
when ``HCP_TEST_KUBECONFIG`` and ``HCP_TEST_K8S_CONTRACT_IMAGE`` are set. The runner is the
stand-in in ``contract-runner/``, which does what ``HCP_TEST_BEHAVIOR`` says. A case an
adapter isn't held to (``features``) is skipped for it, saying why.

The cases cover:
- the capability record, and that every lease here passes it;
- submitting, and submitting the same attempt again;
- events, exit codes, canceling, interruptions from outside, deadlines and memory limits;
- image pull failures;
- what a runner can see;
- cleanup, adoption after an agent restart, and provider retries switched off.
"""

from __future__ import annotations

import json
import time

import pytest
from harnesses import KubernetesHarness, LocalPoolHarness

from hydrocouple_platform.agent.adapters import validate
from hydrocouple_platform.contracts.jobs import FailureClass
from hydrocouple_platform.runner import EXIT_CANCELLED


@pytest.fixture(params=["local-pool", pytest.param("kubernetes", marks=pytest.mark.k8s)])
def harness(request, artifact_dir):
    if request.param == "kubernetes":
        made = KubernetesHarness(request.getfixturevalue("kube"),
                                 request.getfixturevalue("namespace"),
                                 request.getfixturevalue("contract_image"))
    else:
        made = LocalPoolHarness(artifact_dir)
    yield made
    made.close()


def needs(harness, feature: str) -> None:
    if feature not in harness.features:
        pytest.skip(f"{harness.name} is not held to '{feature}'")


def submit(harness, attempt: str, behavior: str | None = "ok", *, env=None, secrets=None,
           **lease_kw) -> str:
    """Submit a lease the adapter admits, as the control plane only leases those."""
    lease = harness.lease(attempt, **lease_kw)
    assert validate(harness.adapter, lease.spec) == []
    env = {**({"HCP_TEST_BEHAVIOR": behavior} if behavior else {}), **(env or {})}
    return harness.adapter.submit(lease, harness.store_uri, env=env, secrets=secrets)


def finish(harness, handle: str, adapter=None):
    """Inspect until the attempt ends; the observation carries every event seen."""
    adapter = adapter or harness.adapter
    events, deadline = [], time.monotonic() + harness.finish_timeout
    while time.monotonic() < deadline:
        obs = adapter.inspect(handle)
        events += obs.events
        if obs.finished:
            obs.events = events
            return obs
        time.sleep(0.25)
    raise AssertionError(f"{handle} did not finish within {harness.finish_timeout} s")


def wait_ready(harness, handle: str) -> list:
    """Wait for the stand-in's "ready" event: it then handles TERM as a runner does."""
    events, deadline = [], time.monotonic() + harness.start_timeout
    while not any(e.phase == "ready" for e in events):
        assert time.monotonic() < deadline, f"{handle} never became ready"
        obs = harness.adapter.inspect(handle)
        assert not obs.finished, f"{handle} ended before it was ready"
        events += obs.events
        time.sleep(0.25)
    return events


def test_capabilities_declare_everything_admission_checks(harness):
    cap = harness.adapter.capabilities()
    assert cap.adapter == harness.adapter.name and cap.job_classes
    assert cap.provider_retries == 0 and cap.idempotent_submit is True
    assert cap.has_runtime_cap is not None and cap.status_integration is not None
    if cap.has_runtime_cap:
        assert cap.max_task_seconds is not None
    for flag in ("max_parallel_tasks", "max_cpu", "max_memory_gib", "gpu",
                 "scratch_in_memory", "interruption_tolerant"):
        assert getattr(cap, flag) is not None, flag
    assert cap.scratch_in_memory or cap.scratch_gib is not None
    # Services with fixed task sizes and request quotas must say so (plan §4).
    if cap.family == "serverless":
        assert cap.task_sizes, "a serverless adapter declares the task sizes it offers"
    if cap.family in ("batch", "serverless"):
        assert cap.submission_rate_per_second, "a cloud adapter declares its submission rate"


def test_a_successful_attempt_reports_its_events(harness):
    handle = submit(harness, "att_ok")
    obs = finish(harness, handle)
    assert obs.exit_code == 0 and obs.failure_class is None
    assert not (obs.cancelled_by_agent or obs.timed_out)
    assert [(e.kind.value, e.phase) for e in obs.events] == [
        ("lifecycle", "started"), ("progress", "running")]
    harness.check_reference("att_ok", handle, obs.provider_reference)


def test_submitting_the_same_attempt_again_starts_nothing(harness):
    handle = submit(harness, "att_again", "sleep")
    assert submit(harness, "att_again", "sleep") == handle
    if "outlives-agent" in harness.features:
        # Another agent instance, or a retry after a lost response, submits it too.
        lease = harness.lease("att_again")
        assert harness.restarted().submit(lease, harness.store_uri,
                                          env={"HCP_TEST_BEHAVIOR": "sleep"}) == handle
    wait_ready(harness, handle)
    assert harness.submitted("att_again") == 1
    harness.adapter.cancel(handle)
    finish(harness, handle)


def test_a_failing_runner_reports_its_exit_code_and_output(harness):
    obs = finish(harness, submit(harness, "att_exit", "exit:12"))
    assert obs.exit_code == 12 and obs.failure_class is None
    assert "a line for the failure tail" in obs.stderr_tail


def test_cancelling_stops_the_runner(harness):
    handle = submit(harness, "att_cancel", "sleep")
    wait_ready(harness, handle)
    harness.adapter.cancel(handle)
    obs = finish(harness, handle)
    assert obs.cancelled_by_agent and not obs.timed_out
    assert harness.running("att_cancel") == 0


def test_a_runner_stopped_from_outside_is_an_interruption(harness):
    """Evicted, preempted or reclaimed: the agent reports an interruption, not a cancel."""
    handle = submit(harness, "att_interrupt", "sleep")
    wait_ready(harness, handle)
    harness.interrupt("att_interrupt")
    obs = finish(harness, handle)
    assert not (obs.cancelled_by_agent or obs.timed_out)
    # The scheduler's own evidence, or the runner's exit after a TERM it didn't ask for.
    assert obs.failure_class == FailureClass.INTERRUPTED or obs.exit_code == EXIT_CANCELLED, obs


def test_the_execution_timeout_is_enforced(harness):
    handle = submit(harness, "att_deadline", "sleep", timeout=harness.deadline_seconds)
    obs = finish(harness, handle)
    assert obs.timed_out and not obs.cancelled_by_agent


def test_a_runner_over_its_memory_limit_fails_as_out_of_memory(harness):
    needs(harness, "memory-limit")
    handle = submit(harness, "att_oom", "oom", memory_gib=0.125)
    # The kernel kills the process using the memory (here a child of the script), and the
    # scheduler records the out-of-memory kill whatever the exit code.
    assert finish(harness, handle).failure_class == FailureClass.OUT_OF_MEMORY


def test_an_image_that_cannot_be_pulled_fails_without_a_retry(harness):
    needs(harness, "images")
    obs = finish(harness, submit(harness, "att_pull", None, image=harness.missing_image()))
    assert obs.failure_class == FailureClass.MISSING_DEPENDENCY
    harness.check_pull_failure(obs)


def test_a_runner_sees_its_spec_and_secrets_but_no_lease_token_or_agent_credential(
        harness, monkeypatch):
    # The agent's own settings, which a runner must never inherit.
    monkeypatch.setenv("HCP_AGENT_CREDENTIAL", "hcpa_agent_secret")
    secrets = {"HCP_RUNNER_TOKEN": "hcpr_contract"}
    handle = submit(harness, "att_inspect", "inspect", job_class="evaluator-pool",
                    env={"HCP_GATEWAY_URL": "http://gateway:8081"}, secrets=secrets)
    obs = finish(harness, handle)
    assert obs.exit_code == 0
    lines = harness.runner_lines(handle)
    seen = dict(line.split(": ", 1) for line in lines if ": " in line)
    assert json.loads(seen["secrets"]) == secrets
    assert (seen["spec-readable"], seen["lease-visible"]) == ("yes", "no")
    assert seen["args"].startswith("-m hydrocouple_platform.runner --spec ")
    env = {line.removeprefix("env: ") for line in lines if line.startswith("env: ")}
    assert env == {"HCP_TEST_BEHAVIOR=inspect", "HCP_GATEWAY_URL=http://gateway:8081",
                   *harness.expected_runner_env()}
    assert not any("token-att_inspect" in line or "hcpa_agent_secret" in line for line in lines)
    harness.check_isolation(seen)


def test_a_restarted_agent_adopts_the_attempts_still_running(harness):
    needs(harness, "outlives-agent")
    handle = submit(harness, "att_adopt", "sleep", job_class="evaluator-pool",
                    secrets={"HCP_RUNNER_TOKEN": "hcpr_adopt"})
    restarted = harness.restarted()
    adopted = {h: (lease, secrets) for lease, h, secrets in restarted.adopt()}
    assert handle in adopted
    assert adopted[handle][0].attempt_token == "token-att_adopt"
    assert adopted[handle][1] == {"HCP_RUNNER_TOKEN": "hcpr_adopt"}
    restarted.cancel(handle)
    assert finish(harness, handle, adapter=restarted).cancelled_by_agent
    restarted.cleanup(handle)


def test_provider_retries_are_off_in_what_the_scheduler_keeps(harness):
    needs(harness, "provider-records")
    handle = submit(harness, "att_retries")
    assert harness.provider_retries("att_retries") == 0
    finish(harness, handle)


def test_cleanup_removes_what_the_attempt_left(harness):
    handle = submit(harness, "att_clean")
    finish(harness, handle)
    harness.adapter.cleanup(handle)
    deadline = time.monotonic() + 60
    while not harness.gone("att_clean", handle):
        assert time.monotonic() < deadline, f"{harness.name} left the attempt's objects"
        time.sleep(0.5)
