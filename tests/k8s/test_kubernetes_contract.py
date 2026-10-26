"""The Kubernetes adapter's contract, against a real cluster.

The runner here is the stand-in image from ``tests/k8s/contract-runner``: it does what
``HCP_TEST_BEHAVIOR`` says (succeed, exit with a code, run until stopped, use too much
memory, or report what it can see), so these tests cover the adapter itself:
- submitting, and submitting the same attempt again;
- reading events and outcomes, cancelling, deadlines and memory limits;
- image pull failures;
- what a runner pod can see;
- cleanup, and adopting running Jobs after an agent restart.

``tests/k8s/test_platform_on_kubernetes.py`` runs real workflows end to end.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from hydrocouple_platform.agent.adapters.kubernetes import (
    KubernetesAdapter,
    KubernetesConfig,
    job_name,
)
from hydrocouple_platform.contracts.agent import LeaseGrant
from hydrocouple_platform.contracts.jobs import FailureClass

pytestmark = pytest.mark.k8s


def make_lease(image: str, attempt: str, timeout: int = 300, memory_gib: float = 0.25,
               job_class: str = "single") -> LeaseGrant:
    return LeaseGrant.model_validate({
        "attempt_id": attempt, "job_id": "job_" + attempt, "fencing_token": 1,
        "attempt_token": "token-" + attempt, "lease_expires_at": datetime.now(timezone.utc),
        "heartbeat_interval_seconds": 15,
        "spec": {
            "job_id": "job_" + attempt, "run_id": "run_1", "organization_id": "org_1",
            "project_id": "prj_1", "actor": {"human_subject": "alice"},
            "workload": {"workflow": "swmm.simulation", "workflow_version": "1"},
            "runtime": {"image": image, "threads": 1},
            "inputs": {"revision_id": "rev_1", "manifest_sha256": "b" * 64,
                       "storage_prefix": "revisions/rev_1/",
                       "files": [{"path": "composition.json", "sha256": "c" * 64, "size": 1}]},
            "resources": {"job_class": job_class, "cpu": 0.25, "memory_gib": memory_gib,
                          "scratch_gib": 1},
            "placement": {"target_id": "tgt_1"},
            "limits": {"execution_timeout_seconds": timeout},
            "outputs": {"storage_prefix": "runs/org_1/prj_1/run_1/",
                        "required_artifacts": ["run.json"]},
        }})


@pytest.fixture
def adapter(kube, namespace):
    cfg = KubernetesConfig(namespace=namespace, max_cpu=2, max_memory_gib=2, scratch_gib=4,
                           poll_seconds=0.5, grace_seconds=5, start_grace_seconds=20)
    made = KubernetesAdapter(cfg, api_client=kube)
    yield made
    for handle in made.handles():
        made.cleanup(handle)


def finish(adapter, handle, timeout=180):
    events, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline:
        obs = adapter.inspect(handle)
        events += obs.events
        if obs.finished:
            obs.events = events
            return obs
        time.sleep(0.5)
    raise AssertionError(f"{handle} did not finish within {timeout} s")


def test_a_successful_attempt_reports_its_events_and_resubmitting_starts_nothing(
        adapter, contract_image, kube, namespace):
    from kubernetes import client as k8s

    lease = make_lease(contract_image, "att_ok", )
    handle = adapter.submit(lease, "file:///store", env={"HCP_TEST_BEHAVIOR": "ok"})
    # Another agent instance (or a retry after a lost response) submits the same attempt.
    again = KubernetesAdapter(adapter.cfg, api_client=kube)
    assert again.submit(lease, "file:///store", env={"HCP_TEST_BEHAVIOR": "ok"}) == handle
    jobs = k8s.BatchV1Api(kube).list_namespaced_job(
        namespace, label_selector="hydrocouple.org/attempt=att_ok").items
    assert len(jobs) == 1
    obs = finish(adapter, handle)
    assert obs.exit_code == 0 and obs.failure_class is None
    assert [(e.kind.value, e.phase) for e in obs.events] == [
        ("lifecycle", "started"), ("progress", "running")]
    assert obs.provider_reference == f"job:{namespace}/{handle}"


def test_a_failing_runner_reports_its_exit_code_and_output(adapter, contract_image):
    handle = adapter.submit(make_lease(contract_image, "att_exit"), "file:///store",
                            env={"HCP_TEST_BEHAVIOR": "exit:12"})
    obs = finish(adapter, handle)
    assert obs.exit_code == 12 and obs.failure_class is None
    assert "a line for the failure tail" in obs.stderr_tail


def test_cancelling_stops_the_pod_and_the_job(adapter, contract_image, kube, namespace):
    from kubernetes import client as k8s

    handle = adapter.submit(make_lease(contract_image, "att_cancel"), "file:///store",
                            env={"HCP_TEST_BEHAVIOR": "sleep"})
    deadline = time.monotonic() + 120
    while adapter.inspect(handle).state.value != "RUNNING":
        assert time.monotonic() < deadline
        time.sleep(0.5)
    adapter.cancel(handle)
    obs = finish(adapter, handle)
    assert obs.cancelled_by_agent
    assert not k8s.BatchV1Api(kube).list_namespaced_job(
        namespace, label_selector="hydrocouple.org/attempt=att_cancel").items


def test_the_execution_timeout_is_the_jobs_deadline(adapter, contract_image):
    handle = adapter.submit(make_lease(contract_image, "att_deadline", timeout=8),
                            "file:///store", env={"HCP_TEST_BEHAVIOR": "sleep"})
    assert finish(adapter, handle).timed_out


def test_a_runner_over_its_memory_limit_fails_as_out_of_memory(adapter, contract_image):
    handle = adapter.submit(make_lease(contract_image, "att_oom", memory_gib=0.125),
                            "file:///store", env={"HCP_TEST_BEHAVIOR": "oom"})
    # The kernel kills the process using the memory (here a child of the script), and the
    # kubelet records the container as OOMKilled whatever the exit code.
    assert finish(adapter, handle).failure_class == FailureClass.OUT_OF_MEMORY


def test_an_image_that_cannot_be_pulled_fails_without_a_retry(adapter, contract_image):
    missing = contract_image.split("@")[0] + "@sha256:" + "0" * 64
    handle = adapter.submit(make_lease(missing, "att_pull"), "file:///store")
    obs = finish(adapter, handle, timeout=120)
    assert obs.failure_class == FailureClass.MISSING_DEPENDENCY
    assert "Err" in obs.stderr_tail or "BackOff" in obs.stderr_tail


def test_a_runner_sees_its_spec_and_secrets_but_no_lease_token_or_credentials(
        adapter, contract_image):
    handle = adapter.submit(
        make_lease(contract_image, "att_inspect", job_class="evaluator-pool"), "file:///store",
        env={"HCP_TEST_BEHAVIOR": "inspect", "HCP_GATEWAY_URL": "http://gateway:8081"},
        secrets={"HCP_RUNNER_TOKEN": "hcpr_contract"})
    obs = finish(adapter, handle)
    assert obs.exit_code == 0
    lines = adapter._tracked[handle].tail
    seen = dict(line.split(": ", 1) for line in lines if ": " in line)
    assert seen["secrets"] == '{"HCP_RUNNER_TOKEN": "hcpr_contract"}'
    assert (seen["spec-readable"], seen["lease-visible"], seen["sa-token"]) == ("yes", "no", "no")
    assert (seen["uid"], seen["root-writable"], seen["work-writable"]) == ("10001", "no", "yes")
    assert seen["args"].startswith("-m hydrocouple_platform.runner --spec ")
    env = {line.removeprefix("env: ") for line in lines if line.startswith("env: ")}
    assert env == {"HCP_TEST_BEHAVIOR=inspect", "HCP_GATEWAY_URL=http://gateway:8081",
                   "HCP_RUNNER_SECRETS_FILE=/var/run/hydrocouple/secrets.json"}


def test_a_restarted_agent_adopts_the_jobs_still_running(adapter, contract_image, kube):
    lease = make_lease(contract_image, "att_adopt", job_class="evaluator-pool")
    handle = adapter.submit(lease, "file:///store", env={"HCP_TEST_BEHAVIOR": "sleep"},
                            secrets={"HCP_RUNNER_TOKEN": "hcpr_adopt"})
    restarted = KubernetesAdapter(adapter.cfg, api_client=kube)  # nothing in memory
    adopted = {h: (lease_, secrets) for lease_, h, secrets in restarted.adopt()}
    assert handle in adopted
    assert adopted[handle][0].attempt_token == "token-att_adopt"
    assert adopted[handle][1] == {"HCP_RUNNER_TOKEN": "hcpr_adopt"}
    restarted.cancel(handle)
    assert finish(restarted, handle).cancelled_by_agent
    restarted.cleanup(handle)


def test_cleanup_removes_the_job_and_its_secret(adapter, contract_image, kube, namespace):
    from kubernetes import client as k8s

    handle = adapter.submit(make_lease(contract_image, "att_clean"), "file:///store",
                            env={"HCP_TEST_BEHAVIOR": "ok"})
    finish(adapter, handle)
    adapter.cleanup(handle)
    deadline = time.monotonic() + 60
    core, batch = k8s.CoreV1Api(kube), k8s.BatchV1Api(kube)
    selector = "hydrocouple.org/attempt=att_clean"
    while (batch.list_namespaced_job(namespace, label_selector=selector).items
           or core.list_namespaced_secret(namespace, label_selector=selector).items):
        assert time.monotonic() < deadline, "the Job or its Secret is still there"
        time.sleep(0.5)
    assert job_name("att_clean") == handle
