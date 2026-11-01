"""One harness per scheduler adapter, for the shared contract suite (``test_adapter_contract``).

A harness makes the adapter, leases for it, and reads what the scheduler itself kept, so
each case can check the scheduler as well as the adapter's report. Every harness has:
- ``adapter``, ``store_uri`` and ``lease(attempt, **kw)``;
- ``submitted(attempt)``: how many times the scheduler took the attempt, counting work it
  still lists after it ended where it keeps any;
- ``running(attempt)``: the attempt's work the scheduler is still running;
- ``interrupt(attempt)``: stops the attempt from outside, as the scheduler itself would
  (an eviction, a preemption, a reclaimed spot instance);
- ``runner_lines(handle)``: what the runner wrote to stderr;
- ``check_reference``, ``check_isolation`` and ``expected_runner_env``: what is the
  adapter's own;
- ``start_timeout``, ``finish_timeout`` and ``deadline_seconds``: the scheduler's pace
  (AWS Batch's shortest deadline is 60 s, for example).

``features`` names the optional cases an adapter is held to:
- ``memory-limit``: the scheduler enforces the memory request;
- ``images``: it runs container images, so an image can fail to pull (``check_pull_failure``);
- ``outlives-agent``: its work survives the agent, which adopts it after a restart
  (``restarted()``);
- ``provider-records``: it keeps objects that can be read back (``provider_retries``).

Every runner here is the stand-in in ``contract-runner/``: the script itself for the local
pool, and the image built from that folder on container schedulers. It writes events to
stdout and reads secrets from a file or stdin, as the runner does today; when the runner
gets other channels (the agent's gateway, a launch object, M5.3), the stand-in follows.
"""

from __future__ import annotations

import os
import signal
from datetime import datetime, timezone
from pathlib import Path

from hydrocouple_platform.contracts.agent import LeaseGrant

STAND_IN = Path(__file__).parent / "contract-runner" / "python"


def make_lease(attempt: str, image: str | None, timeout: int = 300, memory_gib: float = 0.25,
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


class LocalPoolHarness:
    name = "local-pool"
    features = frozenset()
    start_timeout, finish_timeout, deadline_seconds = 30, 60, 8

    def __init__(self, root: Path):
        from hydrocouple_platform.agent.adapters.local import LocalPoolAdapter

        self.work = (root / "work").resolve()
        self.adapter = LocalPoolAdapter(self.work, slots=8, python=str(STAND_IN),
                                        grace_seconds=2, keep_workdirs=False)
        self.store_uri = "file://" + str((root / "store").resolve())

    def lease(self, attempt: str, **kw) -> LeaseGrant:
        return make_lease(attempt, image=None, **kw)

    def _pids(self, attempt: str) -> list[int]:
        """This harness's runner processes for the attempt. A shell's subshells share its
        command line, so only those whose parent isn't one of them count."""
        workdir = str(self.work / attempt).encode()
        parents = {}
        for pid in filter(str.isdigit, os.listdir("/proc")):
            try:
                args = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
                state, ppid = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[:2]
            except OSError:
                continue
            if state != "Z" and _after(args, b"--attempt-id") == attempt.encode() \
                    and _after(args, b"--workdir") == workdir:
                parents[pid] = ppid
        return [int(pid) for pid, ppid in parents.items() if ppid not in parents]

    def submitted(self, attempt: str) -> int:
        return len(self._pids(attempt))  # a process leaves nothing to list once it ends

    def running(self, attempt: str) -> int:
        return len(self._pids(attempt))

    def interrupt(self, attempt: str) -> None:
        """TERM from outside the agent, as a shutting-down machine sends."""
        for pid in self._pids(attempt):
            os.killpg(pid, signal.SIGTERM)

    def runner_lines(self, handle: str) -> list[str]:
        log = self.work / handle / "runner.stderr.log"
        return log.read_text("utf-8").splitlines() if log.exists() else []

    def gone(self, attempt: str, handle: str) -> bool:
        return not (self.work / handle).exists() and self.running(attempt) == 0

    def check_reference(self, attempt: str, handle: str, reference: str) -> None:
        assert handle == attempt and reference.startswith("pid:"), reference

    def expected_runner_env(self) -> set[str]:
        return {"HCP_RUNNER_SECRETS_STDIN=1"}

    def check_isolation(self, seen: dict[str, str]) -> None:
        assert seen["work-writable"] == "yes"

    def close(self) -> None:
        for handle in self.adapter.handles():
            self.adapter.cancel(handle)
            self.adapter.cleanup(handle)


def _after(args: list[bytes], flag: bytes) -> bytes | None:
    return args[args.index(flag) + 1] if flag in args[:-1] else None


class KubernetesHarness:
    name = "kubernetes"
    features = frozenset({"memory-limit", "images", "outlives-agent", "provider-records"})
    store_uri = "file:///store"
    start_timeout, finish_timeout, deadline_seconds = 120, 180, 8

    def __init__(self, kube, namespace: str, image: str):
        from hydrocouple_platform.agent.adapters.kubernetes import (
            KubernetesAdapter,
            KubernetesConfig,
        )

        self.kube, self.namespace, self.image = kube, namespace, image
        self.cfg = KubernetesConfig(namespace=namespace, max_cpu=2, max_memory_gib=2,
                                    scratch_gib=4, poll_seconds=0.5, grace_seconds=5,
                                    start_grace_seconds=20)
        self.adapter = KubernetesAdapter(self.cfg, api_client=kube)

    def lease(self, attempt: str, image: str | None = None, **kw) -> LeaseGrant:
        return make_lease(attempt, image=image or self.image, **kw)

    def missing_image(self) -> str:
        return self.image.split("@")[0] + "@sha256:" + "0" * 64

    def restarted(self):
        """The adapter as a restarted agent makes it: nothing in memory."""
        from hydrocouple_platform.agent.adapters.kubernetes import KubernetesAdapter

        return KubernetesAdapter(self.cfg, api_client=self.kube)

    def _selector(self, attempt: str) -> str:
        return f"hydrocouple.org/attempt={attempt}"

    def _jobs(self, attempt: str) -> list:
        from kubernetes import client as k8s

        return k8s.BatchV1Api(self.kube).list_namespaced_job(
            self.namespace, label_selector=self._selector(attempt)).items

    def submitted(self, attempt: str) -> int:
        return len(self._jobs(attempt))

    def running(self, attempt: str) -> int:
        """Jobs for the attempt that are not being deleted."""
        return sum(1 for job in self._jobs(attempt) if job.metadata.deletion_timestamp is None)

    def interrupt(self, attempt: str) -> None:
        """Evict the runner's pod through the Eviction API, as a node drain does."""
        from kubernetes import client as k8s

        core = k8s.CoreV1Api(self.kube)
        for pod in core.list_namespaced_pod(self.namespace,
                                            label_selector=self._selector(attempt)).items:
            core.create_namespaced_pod_eviction(pod.metadata.name, self.namespace, {
                "metadata": {"name": pod.metadata.name, "namespace": self.namespace}})

    def provider_retries(self, attempt: str) -> int:
        (job,) = self._jobs(attempt)
        # A failed pod fails the Job at once (backoffLimit 0), and so does a disruption,
        # which Kubernetes would otherwise retry outside the backoff limit.
        rules = job.spec.pod_failure_policy.rules
        assert [(r.action, r.on_pod_conditions[0].type) for r in rules] == [
            ("FailJob", "DisruptionTarget")], rules
        return job.spec.backoff_limit

    def check_pull_failure(self, obs) -> None:
        assert "Err" in obs.stderr_tail or "BackOff" in obs.stderr_tail, obs.stderr_tail

    def runner_lines(self, handle: str) -> list[str]:
        return list(self.adapter._tracked[handle].tail)

    def gone(self, attempt: str, handle: str) -> bool:
        from kubernetes import client as k8s

        secrets = k8s.CoreV1Api(self.kube).list_namespaced_secret(
            self.namespace, label_selector=self._selector(attempt)).items
        return not self._jobs(attempt) and not secrets

    def check_reference(self, attempt: str, handle: str, reference: str) -> None:
        from hydrocouple_platform.agent.adapters.kubernetes import job_name

        assert handle == job_name(attempt)
        assert reference == f"job:{self.namespace}/{handle}", reference

    def expected_runner_env(self) -> set[str]:
        return {"HCP_RUNNER_SECRETS_FILE=/var/run/hydrocouple/secrets.json"}

    def check_isolation(self, seen: dict[str, str]) -> None:
        # The restricted Pod Security Standard, a read-only root and no API token.
        assert seen["sa-token"] == "no"
        assert (seen["uid"], seen["root-writable"], seen["work-writable"]) == ("10001", "no", "yes")

    def close(self) -> None:
        for handle in self.adapter.handles():
            self.adapter.cleanup(handle)
