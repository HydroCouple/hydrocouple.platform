"""Kubernetes adapter: each attempt is one ``batch/v1`` Job in the target's namespace.

- Provider retries are off (``backoffLimit: 0``), and the attempt's execution
  timeout is the Job's ``activeDeadlineSeconds``.
- When the target names a Kueue LocalQueue, the Job carries its label and Kueue
  decides when it starts.
- The job spec, and an evaluator's gateway token, reach the runner in a
  per-attempt Secret that the Job owns. The Secret also keeps the lease, so an
  agent that restarts adopts the Jobs it was running.
- Events are read from the pod log; the outcome comes from the Job and pod status.
- Runner pods run unprivileged, with no service-account token, a read-only root
  filesystem and an ``emptyDir`` working directory.

The agent needs only namespaced permissions: Jobs, Pods, ``pods/log`` and
Secrets in its own namespace (``deploy/kustomize``).
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...contracts.agent import Event, LeaseGrant
from ...contracts.capabilities import AdapterCapabilities
from ...contracts.jobs import FailureClass, JobClass
from ...contracts.states import AttemptState
from ...runner import EVENT_PREFIX
from . import Observation

log = logging.getLogger(__name__)

LABEL = "hydrocouple.org"
MANAGED_BY = {f"{LABEL}/managed-by": "hydrocouple-agent"}
KUEUE_QUEUE_LABEL = "kueue.x-k8s.io/queue-name"
CONTAINER = "runner"
ATTEMPT_DIR = "/var/run/hydrocouple"
WORKDIR = "/work"
# Waiting reasons that do not clear by themselves, and how they fail the attempt.
STUCK_REASONS = {
    "InvalidImageName": FailureClass.INVALID_INPUT,
    "ErrImagePull": FailureClass.MISSING_DEPENDENCY,
    "ImagePullBackOff": FailureClass.MISSING_DEPENDENCY,
    "CreateContainerConfigError": FailureClass.UNKNOWN,
    "CreateContainerError": FailureClass.UNKNOWN,
}


# What the agent's Role must allow in its namespace (deploy/kustomize/base/rbac.yaml).
REQUIRED_PERMISSIONS = [
    *[(verb, "batch", "jobs", "") for verb in ("create", "get", "list", "delete")],
    *[(verb, "", "pods", "") for verb in ("get", "list")],
    ("get", "", "pods", "log"),
    *[(verb, "", "secrets", "") for verb in ("create", "get", "list", "patch", "delete")],
]


class StoreVolume(BaseModel):
    """The agent's ``file://`` store on a shared volume (an NFS claim on premises), which
    runners mount at ``mount_path`` and use as ``file://<mount_path>``."""

    model_config = ConfigDict(extra="forbid")

    mount_path: str
    claim_name: str | None = None
    host_path: str | None = None

    @model_validator(mode="after")
    def _one_source(self) -> StoreVolume:
        if (self.claim_name is None) == (self.host_path is None):
            raise ValueError("a store volume names exactly one of claim_name or host_path")
        return self


class GpuType(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource: str = "nvidia.com/gpu"
    node_selector: dict[str, str] = Field(default_factory=dict)


class KubernetesConfig(BaseModel):
    """A Kubernetes target, as its agent is configured (``deploy/kustomize``)."""

    model_config = ConfigDict(extra="forbid")

    namespace: str
    # The largest profile the target approves; requests beyond it are refused before leasing.
    max_cpu: float = Field(gt=0)
    max_memory_gib: float = Field(gt=0)
    scratch_gib: float = Field(gt=0)
    # Attempts this agent runs at once. With Kueue, admission decides when each one starts.
    slots: int = Field(default=8, ge=1)
    queue_name: str | None = None  # a Kueue LocalQueue in the namespace
    runner_service_account: str = "hcp-runner"
    # A Secret whose keys become the runner's environment: credentials for an
    # S3-compatible store on premises. Cloud targets use workload identity instead.
    runner_env_secret: str | None = None
    # Labels and annotations on runner pods, for workload identity: on AKS
    # {"azure.workload.identity/use": "true"}; EKS and GKE need only the ServiceAccount.
    runner_pod_labels: dict[str, str] = Field(default_factory=dict)
    runner_pod_annotations: dict[str, str] = Field(default_factory=dict)
    image_pull_secrets: list[str] = Field(default_factory=list)
    node_selector: dict[str, str] = Field(default_factory=dict)
    tolerations: list[dict[str, Any]] = Field(default_factory=list)
    gpus: dict[str, GpuType] = Field(default_factory=dict)
    interruptible: bool = False  # the nodes are spot or preemptible
    store_volume: StoreVolume | None = None
    run_as_user: int = Field(default=10001, ge=1)
    grace_seconds: int = Field(default=30, ge=1)
    ttl_seconds_after_finished: int = Field(default=86400, ge=0)
    # How long a container may wait on an image pull or its configuration before it fails.
    start_grace_seconds: float = Field(default=600, gt=0)
    poll_seconds: float = Field(default=2.0, gt=0)


def job_name(attempt_id: str) -> str:
    """A DNS-1123 name for an attempt's Job and Secret."""
    name = "hc-" + re.sub(r"[^a-z0-9-]", "-", attempt_id.lower())
    return name[:63].rstrip("-")


def _label_value(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]", "-", value)[:63]
    return value.strip("-_.") or "none"


def labels_for(lease: LeaseGrant) -> dict[str, str]:
    spec = lease.spec
    out = {**MANAGED_BY, f"{LABEL}/org": spec.organization_id,
           f"{LABEL}/project": spec.project_id, f"{LABEL}/run": spec.run_id,
           f"{LABEL}/job": spec.job_id, f"{LABEL}/attempt": lease.attempt_id}
    study = (spec.workload.parameters.get("study") or {}).get("study_id")
    if study:
        out[f"{LABEL}/study"] = study
    return {k: _label_value(v) for k, v in out.items()}


def _quantity(value: float, unit: str = "") -> str:
    return f"{value:g}{unit}"


def build_secret(lease: LeaseGrant, secrets: dict[str, str] | None) -> dict:
    """The per-attempt Secret: the spec and runner secrets for the pod, and the lease for
    the agent. The pod mounts only the first two keys."""
    data = {"spec.json": lease.spec.model_dump_json(),
            "lease.json": lease.model_dump_json()}
    if secrets:
        data["secrets.json"] = json.dumps(secrets)
    return {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": job_name(lease.attempt_id),
                         "labels": {**labels_for(lease), f"{LABEL}/role": "attempt"}},
            "stringData": data}


def _runner_pod(cfg: KubernetesConfig, *, image: str, command: list[str], store_uri: str,
                variables: dict[str, str], amounts: dict[str, str], limits: dict[str, str],
                node_selector: dict[str, str], attempt_secret: tuple[str, list] | None) -> dict:
    """The runner pod: unprivileged, with no service-account token, a read-only root
    filesystem, an emptyDir working directory, and the attempt's Secret when it has one."""
    mounts = [{"name": "work", "mountPath": WORKDIR}, {"name": "tmp", "mountPath": "/tmp"}]
    volumes = [{"name": "work", "emptyDir": {"sizeLimit": amounts["ephemeral-storage"]}},
               {"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}}]
    if attempt_secret is not None:
        secret_name, items = attempt_secret
        mounts.append({"name": "attempt", "mountPath": ATTEMPT_DIR, "readOnly": True})
        volumes.append({"name": "attempt", "secret": {"secretName": secret_name, "items": items,
                                                      "defaultMode": 0o440}})
    if cfg.store_volume is not None:
        sv = cfg.store_volume
        mounts.append({"name": "store", "mountPath": sv.mount_path})
        volumes.append({"name": "store", **(
            {"persistentVolumeClaim": {"claimName": sv.claim_name}} if sv.claim_name else
            {"hostPath": {"path": sv.host_path, "type": "Directory"}})})
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "serviceAccountName": cfg.runner_service_account,
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "terminationGracePeriodSeconds": cfg.grace_seconds,
        "securityContext": {"runAsNonRoot": True, "runAsUser": cfg.run_as_user,
                            "runAsGroup": cfg.run_as_user, "fsGroup": cfg.run_as_user,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{
            "name": CONTAINER, "image": image, "imagePullPolicy": "IfNotPresent",
            "command": [*command, "--store", runner_store_uri(cfg, store_uri)],
            "env": [{"name": k, "value": v} for k, v in sorted(
                {**variables, "PYTHONUNBUFFERED": "1", "HOME": WORKDIR}.items())],
            **({"envFrom": [{"secretRef": {"name": cfg.runner_env_secret}}]}
               if cfg.runner_env_secret else {}),
            "resources": {"requests": amounts, "limits": limits},
            "securityContext": {"allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": mounts,
        }],
        "volumes": volumes,
    }
    if node_selector:
        pod_spec["nodeSelector"] = node_selector
    if cfg.tolerations:
        pod_spec["tolerations"] = cfg.tolerations
    if cfg.image_pull_secrets:
        pod_spec["imagePullSecrets"] = [{"name": n} for n in cfg.image_pull_secrets]
    return pod_spec


def runner_store_uri(cfg: KubernetesConfig, store_uri: str) -> str:
    """The agent's store as runners see it: a shared volume's mount path, or the same URI."""
    return "file://" + cfg.store_volume.mount_path if cfg.store_volume else store_uri


def _job(cfg: KubernetesConfig, name: str, labels: dict[str, str], pod_spec: dict,
         deadline: int) -> dict:
    job_labels = dict(labels)
    if cfg.queue_name:
        job_labels[KUEUE_QUEUE_LABEL] = cfg.queue_name
    template = {"metadata": {"labels": {**cfg.runner_pod_labels, **labels}}, "spec": pod_spec}
    if cfg.runner_pod_annotations:
        template["metadata"]["annotations"] = dict(cfg.runner_pod_annotations)
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": name, "labels": job_labels},
        "spec": {
            "backoffLimit": 0,  # the platform's retry budget is the only one
            "completions": 1, "parallelism": 1,
            "activeDeadlineSeconds": deadline,
            "ttlSecondsAfterFinished": cfg.ttl_seconds_after_finished,
            # An eviction or preemption fails the Job at once and is reported as an interruption.
            "podFailurePolicy": {"rules": [{"action": "FailJob", "onPodConditions": [
                {"type": "DisruptionTarget", "status": "True"}]}]},
            "template": template,
        },
    }


def build_job(cfg: KubernetesConfig, lease: LeaseGrant, store_uri: str,
              env: dict[str, str] | None, has_secrets: bool) -> dict:
    spec = lease.spec
    name = job_name(lease.attempt_id)
    res = spec.resources
    amounts = {"cpu": _quantity(res.cpu), "memory": _quantity(res.memory_gib, "Gi"),
               "ephemeral-storage": _quantity(res.scratch_gib, "Gi")}
    limits = dict(amounts)  # requests equal limits: Guaranteed QoS at pinned threads
    node_selector = dict(cfg.node_selector)
    if res.gpu is not None:
        gpu = cfg.gpus[res.gpu]
        limits[gpu.resource] = "1"
        node_selector.update(gpu.node_selector)
    variables = {**(env or {}), "OMP_NUM_THREADS": str(spec.runtime.threads)}
    items = [{"key": "spec.json", "path": "spec.json"}]
    if has_secrets:
        variables["HCP_RUNNER_SECRETS_FILE"] = f"{ATTEMPT_DIR}/secrets.json"
        items.append({"key": "secrets.json", "path": "secrets.json"})
    pod_spec = _runner_pod(
        cfg, image=spec.runtime.image, store_uri=store_uri, variables=variables,
        command=["python", "-m", "hydrocouple_platform.runner",
                 "--spec", f"{ATTEMPT_DIR}/spec.json", "--attempt-id", lease.attempt_id,
                 "--attempt-number", str(lease.fencing_token), "--workdir", WORKDIR],
        amounts=amounts, limits=limits, node_selector=node_selector,
        attempt_secret=(name, items))
    return _job(cfg, name, {**labels_for(lease), f"{LABEL}/role": "runner"}, pod_spec,
                spec.limits.execution_timeout_seconds)


def build_probe_job(cfg: KubernetesConfig, image: str, store_uri: str, key: str,
                    name: str) -> dict:
    """``agent verify``: a runner pod, exactly as attempts get one, that writes ``key``."""
    amounts = {"cpu": "250m", "memory": "512Mi", "ephemeral-storage": "1Gi"}
    pod_spec = _runner_pod(
        cfg, image=image, store_uri=store_uri, variables={},
        command=["python", "-m", "hydrocouple_platform.runner.probe", "--key", key],
        amounts=amounts, limits=dict(amounts), node_selector=dict(cfg.node_selector),
        attempt_secret=None)
    # Labelled as a runner, so the runners' network policy applies to it too.
    return _job(cfg, name, {**MANAGED_BY, f"{LABEL}/role": "runner", f"{LABEL}/probe": "true"},
                pod_spec, 600)


def _timestamp(value: str) -> tuple[int, int]:
    """An RFC 3339 timestamp from the kubelet as (seconds, nanoseconds), for ordering."""
    head, _, frac = value.rstrip("Z").partition(".")
    seconds = int(datetime.strptime(head, "%Y-%m-%dT%H:%M:%S")
                  .replace(tzinfo=timezone.utc).timestamp())
    return seconds, int((frac + "000000000")[:9])


@dataclass
class _Tracked:
    lease: LeaseGrant
    cancelled: bool = False
    last_log: tuple[int, int] = (0, 0)
    last_log_read: float = 0.0
    seen_at_last: set[str] = field(default_factory=set)
    events: list[Event] = field(default_factory=list)
    tail: list[str] = field(default_factory=list)


class KubernetesAdapter:
    name = "kubernetes"

    def __init__(self, cfg: KubernetesConfig, api_client=None):
        from kubernetes import client as k8s

        self.cfg = cfg
        self._api_client = api_client or load_api_client()
        self._batch = k8s.BatchV1Api(self._api_client)
        self._core = k8s.CoreV1Api(self._api_client)
        self._tracked: dict[str, _Tracked] = {}
        self._jobs: dict[str, Any] = {}
        self._pods: dict[str, list[Any]] = {}
        self._listed_at = 0.0

    # -- contract ----------------------------------------------------------------------

    def capabilities(self) -> AdapterCapabilities:
        cfg = self.cfg
        return AdapterCapabilities(
            adapter=self.name, family="kubernetes",
            job_classes=[JobClass.SINGLE, JobClass.EVALUATOR_POOL, JobClass.ANALYSIS],
            has_runtime_cap=False, max_task_seconds=None, max_parallel_tasks=1,
            max_cpu=cfg.max_cpu, max_memory_gib=cfg.max_memory_gib, gpu=sorted(cfg.gpus),
            scratch_gib=cfg.scratch_gib, scratch_in_memory=False,
            interruption_tolerant=cfg.interruptible, provider_retries=0,
            idempotent_submit=True, status_integration="polling", runs_images=True,
        )

    def free_slots(self) -> int:
        return max(self.cfg.slots - len(self._tracked), 0)

    def submit(self, lease: LeaseGrant, store_uri: str, env: dict[str, str] | None = None,
               secrets: dict[str, str] | None = None) -> str:
        from kubernetes.client.rest import ApiException

        if not lease.spec.runtime.image:
            raise ValueError("the workflow has no image qualified for container targets")
        name = job_name(lease.attempt_id)
        if name in self._tracked:
            return name
        ns = self.cfg.namespace
        try:
            self._core.create_namespaced_secret(ns, build_secret(lease, secrets))
        except ApiException as exc:
            if exc.status != 409:  # already there: a retried submit of the same attempt
                raise
        try:
            job = self._batch.create_namespaced_job(
                ns, build_job(self.cfg, lease, store_uri, env, bool(secrets)))
        except ApiException as exc:
            if exc.status != 409:
                raise
            job = self._batch.read_namespaced_job(name, ns)
        # The Job owns the Secret, so deleting the Job removes both.
        self._core.patch_namespaced_secret(name, ns, {"metadata": {"ownerReferences": [{
            "apiVersion": "batch/v1", "kind": "Job", "name": name,
            "uid": job.metadata.uid, "blockOwnerDeletion": False}]}})
        self._tracked[name] = _Tracked(lease=lease)
        self._listed_at = 0.0
        return name

    def adopt(self) -> list[tuple[LeaseGrant, str, dict[str, str]]]:
        """Attempts this namespace's Jobs are still running for, after an agent restart:
        each lease, its handle, and the secrets its runner holds."""
        out = []
        secrets = self._core.list_namespaced_secret(
            self.cfg.namespace, label_selector=_selector({**MANAGED_BY,
                                                          f"{LABEL}/role": "attempt"}))
        for secret in secrets.items:
            name = secret.metadata.name
            data = secret.data or {}
            try:
                lease = LeaseGrant.model_validate_json(_decode(data.get("lease.json", "")))
                runner_secrets = json.loads(_decode(data.get("secrets.json", "")) or "{}")
            except ValueError:
                log.warning("secret %s holds no readable lease; leaving it", name)
                continue
            if name not in self._tracked:
                self._tracked[name] = _Tracked(lease=lease)
            out.append((lease, name, runner_secrets))
        self._listed_at = 0.0
        return out

    def inspect(self, handle: str) -> Observation:
        tracked = self._tracked[handle]
        self._refresh()
        job = self._jobs.get(handle)
        pods = self._pods.get(tracked.lease.attempt_id, [])
        pod = max(pods, key=lambda p: p.metadata.creation_timestamp) if pods else None
        reference = f"job:{self.cfg.namespace}/{handle}"
        if job is None:
            self._read_logs(tracked, pod, final=True)
            if tracked.cancelled:
                return self._obs(tracked, AttemptState.FINALIZING, finished=True,
                                 cancelled_by_agent=True, provider_reference=reference)
            return self._obs(tracked, AttemptState.FINALIZING, finished=True,
                             failure_class=FailureClass.LOST, provider_reference=reference,
                             stderr_tail="the Job was deleted outside the agent")
        conditions = {c.type: c for c in (job.status.conditions or []) if c.status == "True"}
        finished = "Complete" in conditions or "Failed" in conditions
        self._read_logs(tracked, pod, final=finished)
        if "Complete" in conditions:
            return self._obs(tracked, AttemptState.FINALIZING, finished=True, exit_code=0,
                             provider_reference=reference)
        if "Failed" in conditions:
            return self._failed(tracked, conditions["Failed"], pod, reference)
        if tracked.cancelled:
            return self._obs(tracked, AttemptState.RUNNING, provider_reference=reference)
        if job.spec.suspend:  # waiting for Kueue to admit it
            return self._obs(tracked, AttemptState.QUEUED, provider_reference=reference)
        return self._pending_or_running(tracked, pod, reference)

    def cancel(self, handle: str) -> None:
        from kubernetes.client.rest import ApiException

        tracked = self._tracked.get(handle)
        if tracked is None or tracked.cancelled:
            return
        tracked.cancelled = True
        try:
            # Foreground: the Job stays visible until its pod has stopped.
            self._batch.delete_namespaced_job(handle, self.cfg.namespace,
                                              propagation_policy="Foreground")
        except ApiException as exc:
            if exc.status != 404:
                raise
        self._listed_at = 0.0

    def cleanup(self, handle: str) -> None:
        from kubernetes.client.rest import ApiException

        self._tracked.pop(handle, None)
        for delete in (lambda: self._batch.delete_namespaced_job(
                           handle, self.cfg.namespace, propagation_policy="Background"),
                       lambda: self._core.delete_namespaced_secret(handle, self.cfg.namespace)):
            try:
                delete()
            except ApiException as exc:
                if exc.status != 404:
                    log.warning("cleanup of %s failed: %s", handle, exc)
        self._listed_at = 0.0

    def handles(self) -> list[str]:
        return list(self._tracked)

    # -- onboarding (hcp agent --verify) ------------------------------------------------

    def missing_permissions(self) -> list[str]:
        """The namespaced permissions this agent needs and lacks, by SelfSubjectAccessReview."""
        from kubernetes import client as k8s

        auth = k8s.AuthorizationV1Api(self._api_client)
        missing = []
        for verb, group, resource, sub in REQUIRED_PERMISSIONS:
            review = auth.create_self_subject_access_review({
                "apiVersion": "authorization.k8s.io/v1", "kind": "SelfSubjectAccessReview",
                "spec": {"resourceAttributes": {
                    "namespace": self.cfg.namespace, "verb": verb, "group": group,
                    "resource": resource, "subresource": sub}}})
            if not review.status.allowed:
                missing.append(f"{verb} {resource}{'/' + sub if sub else ''}")
        return missing

    def run_probe(self, image: str, store_uri: str, key: str,
                  timeout: float = 300) -> tuple[bool, str]:
        """Run the verification pod; returns whether it succeeded and its log tail."""
        import uuid

        from kubernetes.client.rest import ApiException

        name = f"hc-verify-{uuid.uuid4().hex[:8]}"
        ns = self.cfg.namespace
        self._batch.create_namespaced_job(ns, build_probe_job(self.cfg, image, store_uri, key,
                                                              name))
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                job = self._batch.read_namespaced_job(name, ns)
                done = {c.type for c in (job.status.conditions or []) if c.status == "True"}
                if done & {"Complete", "Failed"}:
                    pods = self._core.list_namespaced_pod(
                        ns, label_selector=f"batch.kubernetes.io/job-name={name}").items
                    tail = ""
                    if pods:
                        try:
                            tail = self._core.read_namespaced_pod_log(
                                pods[0].metadata.name, ns, container=CONTAINER, tail_lines=20)
                        except ApiException:
                            pass
                    return "Complete" in done, tail.strip()
                time.sleep(1)
            return False, f"the verification pod did not finish within {timeout:g} s"
        finally:
            self._batch.delete_namespaced_job(name, ns, propagation_policy="Background")


    # -- status ---------------------------------------------------------------------------

    def _refresh(self) -> None:
        """One list of this agent's Jobs and pods per poll interval, for every attempt."""
        if time.monotonic() - self._listed_at < self.cfg.poll_seconds:
            return
        selector = _selector(MANAGED_BY)
        ns = self.cfg.namespace
        self._jobs = {j.metadata.name: j for j in
                      self._batch.list_namespaced_job(ns, label_selector=selector).items}
        pods: dict[str, list[Any]] = {}
        for p in self._core.list_namespaced_pod(ns, label_selector=selector).items:
            pods.setdefault((p.metadata.labels or {}).get(f"{LABEL}/attempt", ""), []).append(p)
        self._pods = pods
        self._listed_at = time.monotonic()

    def _failed(self, tracked: _Tracked, condition, pod, reference: str) -> Observation:
        if condition.reason == "DeadlineExceeded":
            return self._obs(tracked, AttemptState.FINALIZING, finished=True, timed_out=True,
                             provider_reference=reference)
        terminated = _terminated(pod)
        failure = None
        if pod is not None and (_has_condition(pod, "DisruptionTarget")
                                or pod.status.reason == "Evicted"):
            failure = FailureClass.INTERRUPTED
        elif terminated is not None and terminated.reason == "OOMKilled":
            failure = FailureClass.OUT_OF_MEMORY
        exit_code = terminated.exit_code if terminated is not None else None
        if exit_code is None and failure is None:
            failure = FailureClass.UNKNOWN
        return self._obs(tracked, AttemptState.FINALIZING, finished=True, exit_code=exit_code,
                         failure_class=failure, provider_reference=reference,
                         stderr_tail="\n".join(tracked.tail)[-2000:] or condition.message)

    def _pending_or_running(self, tracked: _Tracked, pod, reference: str) -> Observation:
        if pod is None:
            return self._obs(tracked, AttemptState.STARTING, provider_reference=reference)
        waiting = _waiting(pod)
        if waiting is not None and waiting.reason in STUCK_REASONS:
            age = (datetime.now(timezone.utc) - pod.metadata.creation_timestamp).total_seconds()
            if age > self.cfg.start_grace_seconds:  # the agent's cleanup deletes the Job
                return self._obs(tracked, AttemptState.FINALIZING, finished=True,
                                 failure_class=STUCK_REASONS[waiting.reason],
                                 provider_reference=reference,
                                 stderr_tail=f"{waiting.reason}: {waiting.message or ''}")
        if pod.status.phase == "Running":
            return self._obs(tracked, AttemptState.RUNNING, provider_reference=reference)
        if _has_condition(pod, "PodScheduled", "False"):
            return self._obs(tracked, AttemptState.QUEUED, provider_reference=reference)
        return self._obs(tracked, AttemptState.STARTING, provider_reference=reference)

    @staticmethod
    def _obs(tracked: _Tracked, state: AttemptState, **kw) -> Observation:
        events, tracked.events = tracked.events, []
        return Observation(state=state, events=events, **kw)

    def _read_logs(self, tracked: _Tracked, pod, final: bool) -> None:
        """New runner events from the pod log; non-event lines become the failure tail."""
        from kubernetes.client.rest import ApiException

        if pod is None or _container_status(pod) is None:
            return
        now = time.monotonic()
        if not final and now - tracked.last_log_read < self.cfg.poll_seconds:
            return
        since = None
        if tracked.last_log != (0, 0):
            since = max(int(time.time() - tracked.last_log[0]) + 5, 1)
        try:
            text = self._core.read_namespaced_pod_log(
                pod.metadata.name, self.cfg.namespace, container=CONTAINER, timestamps=True,
                since_seconds=since)
        except ApiException as exc:
            if exc.status not in (400, 404):  # 400: the container has not started
                log.warning("reading the log of %s failed: %s", pod.metadata.name, exc)
            return
        tracked.last_log_read = now
        for line in text.splitlines():
            stamp, _, body = line.partition(" ")
            try:
                ts = _timestamp(stamp)
            except ValueError:
                continue
            if ts < tracked.last_log or (ts == tracked.last_log and body in tracked.seen_at_last):
                continue
            if ts > tracked.last_log:
                tracked.last_log, tracked.seen_at_last = ts, set()
            tracked.seen_at_last.add(body)
            if body.startswith(EVENT_PREFIX):
                try:
                    tracked.events.append(Event.model_validate(
                        json.loads(body[len(EVENT_PREFIX):])))
                except ValueError:
                    pass
            else:
                tracked.tail = (tracked.tail + [body])[-40:]


# -- helpers ---------------------------------------------------------------------------


def load_api_client(kubeconfig: str | None = None):
    """In-cluster credentials when the agent runs in a pod, otherwise a kubeconfig."""
    from kubernetes import client as k8s
    from kubernetes import config as k8s_config

    if kubeconfig:
        return k8s_config.new_client_from_config(config_file=kubeconfig)
    try:
        k8s_config.load_incluster_config()
        return k8s.ApiClient()
    except k8s_config.ConfigException:
        return k8s_config.new_client_from_config()


def _selector(labels: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in labels.items())


def _decode(value: str) -> str:
    import base64

    return base64.b64decode(value).decode("utf-8") if value else ""


def _container_status(pod):
    for status in pod.status.container_statuses or []:
        if status.name == CONTAINER:
            return status
    return None


def _terminated(pod):
    status = _container_status(pod) if pod is not None else None
    if status is None:
        return None
    return (status.state and status.state.terminated) or (
        status.last_state and status.last_state.terminated)


def _waiting(pod):
    status = _container_status(pod)
    return status.state.waiting if status is not None and status.state else None


def _has_condition(pod, kind: str, status: str = "True") -> bool:
    return any(c.type == kind and c.status == status for c in pod.status.conditions or [])
