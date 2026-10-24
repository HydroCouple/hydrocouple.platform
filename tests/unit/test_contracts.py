"""Contracts: lifecycle rules, safe paths, capability checks, manifests, egress."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydrocouple_platform.agent.egress import filter_diagnostics, filter_event
from hydrocouple_platform.contracts.agent import Event, EventKind
from hydrocouple_platform.contracts.capabilities import check_spec
from hydrocouple_platform.contracts.jobs import (
    Actor,
    EgressPolicy,
    Inputs,
    JobClass,
    JobSpec,
    Limits,
    Outputs,
    Placement,
    Resources,
    Runtime,
    Workload,
)
from hydrocouple_platform.contracts.manifests import check_safe_path, sha256_of
from hydrocouple_platform.contracts.states import (
    ATTEMPT_TERMINAL,
    ATTEMPT_TRANSITIONS,
    JOB_HELD,
    JOB_TERMINAL,
    JOB_TRANSITIONS,
    AttemptState,
    JobState,
    is_forward_progress,
    job_can,
)
from world import local_capabilities


def spec(**over) -> JobSpec:
    parts = dict(
        job_id="job_1", run_id="run_1", organization_id="org_1", project_id="prj_1",
        actor=Actor(human_subject="alice"),
        workload=Workload(workflow="swmm.simulation", workflow_version="1"),
        runtime=Runtime(),
        inputs=Inputs(revision_id="rev_1", manifest_sha256="a" * 64, storage_prefix="revisions/x/",
                      files=[{"path": "composition.json", "sha256": "b" * 64, "size": 1}]),
        resources=Resources(), placement=Placement(target_id="tgt_1"), limits=Limits(),
        outputs=Outputs(storage_prefix="runs/x/", required_artifacts=["run.json"]),
    )
    parts.update(over)
    return JobSpec(**parts)


class TestLifecycle:
    def test_terminal_states_have_no_exits(self):
        assert all(not JOB_TRANSITIONS[s] for s in JOB_TERMINAL)
        assert all(not ATTEMPT_TRANSITIONS[s] for s in ATTEMPT_TERMINAL)

    def test_every_held_state_can_retry_fail_or_cancel(self):
        for state in JOB_HELD:
            for exit_ in (JobState.LEASABLE, JobState.FAILED, JobState.CANCELLED):
                assert job_can(state, exit_), (state, exit_)

    def test_accepted_work_cannot_skip_the_lease(self):
        assert not job_can(JobState.ACCEPTED, JobState.RUNNING)
        assert not job_can(JobState.LEASABLE, JobState.SUCCEEDED)
        assert not job_can(JobState.PENDING_APPROVAL, JobState.LEASABLE)

    def test_progress_never_moves_backwards(self):
        assert is_forward_progress(AttemptState.LEASED, AttemptState.RUNNING)
        assert not is_forward_progress(AttemptState.RUNNING, AttemptState.QUEUED)
        assert not is_forward_progress(AttemptState.RUNNING, AttemptState.RUNNING)
        assert not is_forward_progress(AttemptState.SUCCEEDED, AttemptState.RUNNING)


class TestSafePaths:
    @pytest.mark.parametrize("path", ["composition.json", "model/rain/2-yr storm.dat",
                                      "model/external/0-x.dat"])
    def test_accepts_relative_paths(self, path):
        assert check_safe_path(path) == path

    @pytest.mark.parametrize("path", ["", "/etc/passwd", "../x", "a/../../b", "a//b", "./a",
                                      "a/", "C:/x", "a\\b", "a\x00b"])
    def test_rejects_escapes(self, path):
        with pytest.raises(ValueError):
            check_safe_path(path)


class TestCapabilities:
    def test_local_pool_accepts_a_default_simulation(self):
        assert check_spec(local_capabilities(), spec()) == []

    def test_runtime_cap_is_an_admission_check(self):
        cap = local_capabilities(adapter="cloud-run-jobs", family="serverless",
                                 has_runtime_cap=True, max_task_seconds=3600)
        found = check_spec(cap, spec(limits=Limits(estimated_runtime_seconds=7200)))
        assert any("exceeds the 3600 s task cap" in f for f in found)

    def test_unknown_capabilities_reject(self):
        cap = local_capabilities(has_runtime_cap=None, max_memory_gib=None, provider_retries=None)
        found = " | ".join(check_spec(cap, spec()))
        assert "has not declared whether it caps task runtime" in found
        assert "memory (GiB) limit" in found
        assert "provider retries" in found

    def test_container_adapters_need_a_qualified_image(self):
        cap = local_capabilities(adapter="kubernetes", family="kubernetes", runs_images=True)
        assert any("no qualified image" in f for f in check_spec(cap, spec()))

    def test_job_class_gpu_and_interruptible(self):
        found = check_spec(local_capabilities(), spec(
            resources=Resources(job_class=JobClass.MULTI_RANK, gpu="nvidia-l4"),
            placement=Placement(target_id="t", interruption="interruptible")))
        joined = " | ".join(found)
        assert "multi-rank" in joined and "nvidia-l4" in joined and "interruptible" in joined


class TestManifests:
    def test_digest_ignores_key_order(self):
        assert sha256_of({"a": 1, "b": [1, 2]}) == sha256_of({"b": [1, 2], "a": 1})


class TestEgress:
    def _event(self, kind, message="flooding at J-12"):
        return Event(event_id="e" * 16, sequence=1, kind=kind, message=message,
                     completed=10.0, total=20.0, occurred_at=datetime.now(timezone.utc))

    def test_metrics_only_keeps_numbers_and_drops_text(self):
        progress = filter_event(self._event(EventKind.PROGRESS), EgressPolicy.METRICS_ONLY)
        assert progress.message is None and progress.completed == 10.0
        assert filter_event(self._event(EventKind.WARNING), EgressPolicy.METRICS_ONLY) is None
        assert filter_diagnostics({"exit_code": 12, "message": "J-12"},
                                  EgressPolicy.METRICS_ONLY) == {"exit_code": 12}

    def test_summaries_keep_warnings_but_not_logs(self):
        assert filter_event(self._event(EventKind.WARNING), EgressPolicy.SUMMARIES).message
        assert filter_event(self._event(EventKind.LOG), EgressPolicy.SUMMARIES) is None
        long = filter_event(self._event(EventKind.WARNING, "x" * 2000), EgressPolicy.SUMMARIES)
        assert len(long.message) == 500


class TestPublishedSchemas:
    def test_published_schemas_match_the_contracts(self):
        from hydrocouple_platform.schemas import PLATFORM_DIR, render

        for name, text in render().items():
            published = PLATFORM_DIR / name
            assert published.exists(), f"run python -m hydrocouple_platform.schemas ({name})"
            assert published.read_text() == text, f"{name} is stale; regenerate it"

    def test_vendored_hydrocouple_schemas_are_unchanged_copies(self):
        """Copied from HydroCoupleSDK share/schema on 2026-10-24; update both together."""
        import hashlib
        from importlib import resources

        pinned = {
            "composition-1.1.json":
                "8295efdaf1f3ab912a28230684a461c98e47bc5ef92690b1a4bb183387a817ac",
            "run-1.0.json": "7bab8e97a28b483d5ba6f13f43c8fff767730f3e919018a66fe1960e4e3ed4e2",
        }
        base = resources.files("hydrocouple_platform") / "schemas" / "hydrocouple"
        for name, digest in pinned.items():
            assert hashlib.sha256((base / name).read_bytes()).hexdigest() == digest, name
