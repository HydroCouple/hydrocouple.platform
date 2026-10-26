"""The Kubernetes adapter without a cluster: the manifests it writes and how it reads status.

``tests/k8s/test_kubernetes_contract.py`` runs the same adapter against a real cluster.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

import pytest

pytest.importorskip("kubernetes")

from hydrocouple_platform.agent.adapters.kubernetes import (  # noqa: E402
    KUEUE_QUEUE_LABEL,
    KubernetesAdapter,
    KubernetesConfig,
    _timestamp,
    _Tracked,
    build_job,
    build_secret,
    job_name,
)
from hydrocouple_platform.contracts.agent import LeaseGrant  # noqa: E402
from hydrocouple_platform.contracts.capabilities import check_spec  # noqa: E402
from hydrocouple_platform.contracts.jobs import FailureClass  # noqa: E402
from hydrocouple_platform.contracts.states import AttemptState  # noqa: E402

IMAGE = "registry.example/hydrocouple/runner@sha256:" + "a" * 64


def lease(**resources) -> LeaseGrant:
    return LeaseGrant.model_validate({
        "attempt_id": "att_0123456789abcdef", "job_id": "job_1", "fencing_token": 2,
        "attempt_token": "tok", "lease_expires_at": datetime.now(timezone.utc),
        "heartbeat_interval_seconds": 15,
        "spec": {
            "job_id": "job_1", "run_id": "run_1", "organization_id": "org_1",
            "project_id": "prj_1", "actor": {"human_subject": "alice"},
            "workload": {"workflow": "gym.evaluate", "workflow_version": "1",
                         "parameters": {"study": {"study_id": "std_1"}}},
            "runtime": {"image": IMAGE, "threads": 2},
            "inputs": {"revision_id": "rev_1", "manifest_sha256": "b" * 64,
                       "storage_prefix": "revisions/rev_1/",
                       "files": [{"path": "composition.json", "sha256": "c" * 64, "size": 1}]},
            "resources": {"cpu": 2, "memory_gib": 4, "scratch_gib": 8, **resources},
            "placement": {"target_id": "tgt_1"},
            "limits": {"execution_timeout_seconds": 600},
            "outputs": {"storage_prefix": "runs/org_1/prj_1/run_1/",
                        "required_artifacts": ["run.json"]},
        }})


CFG = KubernetesConfig(namespace="hc", max_cpu=8, max_memory_gib=32, scratch_gib=100,
                       queue_name="hydrocouple")


def test_each_attempt_is_one_job_with_retries_off_and_its_deadline():
    job = build_job(CFG, lease(), "s3://bucket/prefix", {"HCP_GATEWAY_URL": "http://agent"},
                    has_secrets=True)
    spec = job["spec"]
    assert job["metadata"]["name"] == "hc-att-0123456789abcdef" == job_name("att_0123456789abcdef")
    assert spec["backoffLimit"] == 0 and spec["completions"] == spec["parallelism"] == 1
    assert spec["activeDeadlineSeconds"] == 600
    assert spec["podFailurePolicy"]["rules"][0]["action"] == "FailJob"
    labels = job["metadata"]["labels"]
    assert labels[KUEUE_QUEUE_LABEL] == "hydrocouple"
    assert labels["hydrocouple.org/study"] == "std_1"
    assert labels["hydrocouple.org/attempt"] == "att_0123456789abcdef"
    # Kueue reads its label from the Job; pods carry the platform labels only.
    assert KUEUE_QUEUE_LABEL not in spec["template"]["metadata"]["labels"]


def test_runner_pods_are_unprivileged_and_see_only_their_spec_and_secrets():
    pod = build_job(CFG, lease(), "s3://b/p", None, has_secrets=True)["spec"]["template"]["spec"]
    (container,) = pod["containers"]
    assert pod["automountServiceAccountToken"] is False and pod["restartPolicy"] == "Never"
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    assert container["securityContext"] == {"allowPrivilegeEscalation": False,
                                            "readOnlyRootFilesystem": True,
                                            "capabilities": {"drop": ["ALL"]}}
    attempt = next(v for v in pod["volumes"] if v["name"] == "attempt")
    assert [i["key"] for i in attempt["secret"]["items"]] == ["spec.json", "secrets.json"]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["HCP_RUNNER_SECRETS_FILE"] == "/var/run/hydrocouple/secrets.json"
    assert env["OMP_NUM_THREADS"] == "2" and not any(k.startswith("HCP_AGENT") for k in env)
    assert container["image"] == IMAGE
    assert container["command"][:3] == ["python", "-m", "hydrocouple_platform.runner"]
    # Requests equal limits: Guaranteed QoS for pinned threads.
    assert container["resources"]["requests"] == container["resources"]["limits"] == {
        "cpu": "2", "memory": "4Gi", "ephemeral-storage": "8Gi"}


def test_the_secret_keeps_the_lease_for_the_agent_and_secrets_only_when_there_are_some():
    with_token = build_secret(lease(), {"HCP_RUNNER_TOKEN": "hcpr_x"})["stringData"]
    assert set(with_token) == {"spec.json", "lease.json", "secrets.json"}
    assert LeaseGrant.model_validate_json(with_token["lease.json"]).attempt_id == \
        "att_0123456789abcdef"
    assert "secrets.json" not in build_secret(lease(), None)["stringData"]
    pod = build_job(CFG, lease(), "s3://b/p", None, has_secrets=False)["spec"]["template"]["spec"]
    attempt = next(v for v in pod["volumes"] if v["name"] == "attempt")
    assert [i["key"] for i in attempt["secret"]["items"]] == ["spec.json"]


def test_a_shared_store_volume_and_gpus_and_node_placement():
    cfg = KubernetesConfig(
        namespace="hc", max_cpu=8, max_memory_gib=32, scratch_gib=100,
        store_volume={"mount_path": "/data/store", "claim_name": "hc-store"},
        gpus={"a100": {"node_selector": {"accelerator": "a100"}}},
        node_selector={"pool": "hydrocouple"}, tolerations=[{"key": "spot", "operator": "Exists"}],
        image_pull_secrets=["mirror"])
    pod = build_job(cfg, lease(gpu="a100"), "file:///data/store", None, False)[
        "spec"]["template"]["spec"]
    assert {"name": "store", "persistentVolumeClaim": {"claimName": "hc-store"}} in pod["volumes"]
    assert {"name": "store", "mountPath": "/data/store"} in pod["containers"][0]["volumeMounts"]
    assert pod["containers"][0]["resources"]["limits"]["nvidia.com/gpu"] == "1"
    assert pod["nodeSelector"] == {"pool": "hydrocouple", "accelerator": "a100"}
    assert pod["tolerations"] and pod["imagePullSecrets"] == [{"name": "mirror"}]
    with pytest.raises(ValueError):
        KubernetesConfig(namespace="hc", max_cpu=1, max_memory_gib=1, scratch_gib=1,
                         store_volume={"mount_path": "/x"})


def test_capabilities_come_from_the_targets_approved_profile():
    caps = KubernetesAdapter(CFG, api_client=object()).capabilities()
    assert caps.family == "kubernetes" and caps.provider_retries == 0 and caps.runs_images
    assert (caps.max_cpu, caps.max_memory_gib, caps.scratch_gib) == (8, 32, 100)
    assert check_spec(caps, lease().spec) == []
    assert check_spec(caps, lease(cpu=16).spec)  # beyond the profile: refused before leasing
    assert check_spec(caps, lease(gpu="a100").spec)  # no GPU types configured


def test_kubelet_timestamps_order_with_trimmed_nanoseconds():
    assert _timestamp("2026-10-26T16:52:34.5Z") > _timestamp("2026-10-26T16:52:34.45Z")
    assert _timestamp("2026-10-26T16:52:35Z") > _timestamp("2026-10-26T16:52:34.999999999Z")


# -- reading status ------------------------------------------------------------------------


def _adapter_with(job, pods, **cfg) -> KubernetesAdapter:
    adapter = KubernetesAdapter(CFG.model_copy(update=cfg), api_client=object())
    handle = job_name("att_0123456789abcdef")
    adapter._tracked[handle] = _Tracked(lease=lease())
    adapter._jobs = {handle: job} if job is not None else {}
    adapter._pods = {"att_0123456789abcdef": pods}
    adapter._listed_at = time.monotonic() + 3600  # use these, never list
    adapter._read_logs = lambda *a, **k: None
    return adapter


def _job(conditions=(), suspend=False):
    return NS(spec=NS(suspend=suspend), status=NS(conditions=[
        NS(type=t, status="True", reason=r, message=f"{t} {r}") for t, r in conditions]))


def _pod(phase="Running", waiting=None, terminated=None, conditions=(), reason=None, age=0):
    state = NS(waiting=NS(reason=waiting, message="no such image") if waiting else None,
               terminated=terminated, running=None)
    return NS(metadata=NS(name="hc-pod", labels={},
                          creation_timestamp=datetime.now(timezone.utc) - timedelta(seconds=age)),
              status=NS(phase=phase, reason=reason,
                        conditions=[NS(type=t, status=s) for t, s in conditions],
                        container_statuses=[NS(name="runner", state=state, last_state=None)]))


@pytest.mark.parametrize("job, pods, expected", [
    (_job(suspend=True), [], AttemptState.QUEUED),  # Kueue has not admitted it
    (_job(), [_pod("Pending", conditions=[("PodScheduled", "False")])], AttemptState.QUEUED),
    (_job(), [_pod("Pending")], AttemptState.STARTING),
    (_job(), [_pod("Running")], AttemptState.RUNNING),
])
def test_waiting_and_running_states(job, pods, expected):
    handle = job_name("att_0123456789abcdef")
    obs = _adapter_with(job, pods).inspect(handle)
    assert obs.state == expected and not obs.finished


def test_outcomes_the_scheduler_evidences():
    handle = job_name("att_0123456789abcdef")
    oom = NS(exit_code=137, reason="OOMKilled")
    cases = [
        (_job([("Complete", None)]), [_pod("Succeeded")], dict(exit_code=0)),
        (_job([("Failed", "DeadlineExceeded")]), [], dict(timed_out=True)),
        (_job([("Failed", "BackoffLimitExceeded")]), [_pod("Failed", terminated=oom)],
         dict(failure_class=FailureClass.OUT_OF_MEMORY, exit_code=137)),
        (_job([("Failed", "PodFailurePolicy")]),
         [_pod("Failed", terminated=NS(exit_code=143, reason="Error"),
               conditions=[("DisruptionTarget", "True")])],
         dict(failure_class=FailureClass.INTERRUPTED)),
        (_job([("Failed", "BackoffLimitExceeded")]),
         [_pod("Failed", terminated=NS(exit_code=12, reason="Error"))],
         dict(exit_code=12, failure_class=None)),
        (None, [], dict(failure_class=FailureClass.LOST)),  # deleted outside the agent
    ]
    for job, pods, expected in cases:
        obs = _adapter_with(job, pods).inspect(handle)
        assert obs.finished
        for key, value in expected.items():
            assert getattr(obs, key) == value, (key, expected)


def test_an_image_that_cannot_be_pulled_fails_after_the_start_grace():
    handle = job_name("att_0123456789abcdef")
    young = _adapter_with(_job(), [_pod("Pending", waiting="ImagePullBackOff", age=10)],
                          start_grace_seconds=60).inspect(handle)
    assert not young.finished  # registries have outages; wait first
    old = _adapter_with(_job(), [_pod("Pending", waiting="ImagePullBackOff", age=120)],
                        start_grace_seconds=60).inspect(handle)
    assert old.finished and old.failure_class == FailureClass.MISSING_DEPENDENCY
    assert "ImagePullBackOff" in old.stderr_tail
