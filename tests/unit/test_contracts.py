"""Contracts: lifecycle rules, safe paths, capability checks, manifests, egress."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hydrocouple_platform.agent.egress import filter_diagnostics, filter_event
from hydrocouple_platform.contracts.agent import Event, EventKind
from hydrocouple_platform.contracts.capabilities import TaskSize, check_spec, fit_task_size
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
from hydrocouple_platform.contracts.manifests import (
    ArtifactEntry,
    AttemptManifest,
    Provenance,
    RuntimeVersions,
    check_safe_path,
    sha256_of,
)
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

    def test_scratch_held_in_memory_counts_against_memory(self):
        """Plan §4, serverless rule 2: on Cloud Run the filesystem is memory."""
        cap = local_capabilities(adapter="cloud-run-jobs", family="serverless",
                                 max_memory_gib=4, scratch_gib=None, scratch_in_memory=True)
        found = check_spec(cap, spec(resources=Resources(memory_gib=2, scratch_gib=3)))
        assert found == ["requested memory (GiB) 5 (2 and 3 GiB of scratch held in memory) "
                         "exceeds cloud-run-jobs limit 4"]
        assert check_spec(cap, spec(resources=Resources(memory_gib=2, scratch_gib=1))) == []

    def test_a_service_with_fixed_task_sizes_needs_one_that_fits(self):
        """Fargate offers set CPU and memory pairs, not any combination up to the maxima."""
        sizes = [{"cpu": 0.25, "memory_gib": 0.5}, {"cpu": 1, "memory_gib": 2},
                 {"cpu": 4, "memory_gib": 8}]
        cap = local_capabilities(adapter="ecs-fargate", family="serverless", max_cpu=4,
                                 max_memory_gib=8, task_sizes=sizes)
        # A request runs on the smallest size with enough of both, so these fit.
        for cpu, memory in ((0.5, 2), (0.25, 4), (4, 8)):
            assert check_spec(cap, spec(resources=Resources(cpu=cpu, memory_gib=memory))) == []
        found = check_spec(cap, spec(resources=Resources(cpu=2, memory_gib=6, scratch_gib=1)))
        assert found == []
        cap = cap.model_copy(update={"max_memory_gib": 16})
        found = check_spec(cap, spec(resources=Resources(cpu=1, memory_gib=12)))
        assert found == ["no ecs-fargate task size has 1 CPU and 12 GiB of memory; "
                         "its sizes go up to 4 CPU and up to 8 GiB"]

    def test_in_memory_scratch_and_task_sizes_together(self):
        sizes = [{"cpu": 1, "memory_gib": 2}, {"cpu": 2, "memory_gib": 4}]
        cap = local_capabilities(adapter="cloud-run-jobs", family="serverless", max_cpu=2,
                                 max_memory_gib=4, scratch_gib=None, scratch_in_memory=True,
                                 task_sizes=sizes)
        res = Resources(cpu=1, memory_gib=2, scratch_gib=1)  # 3 GiB in all: the 2 CPU size
        assert check_spec(cap, spec(resources=res)) == []
        assert fit_task_size(cap, res) == TaskSize(cpu=2, memory_gib=4)
        assert fit_task_size(cap, Resources(cpu=0.5, memory_gib=1, scratch_gib=0)) == \
            TaskSize(cpu=1, memory_gib=2)

    def test_the_closest_miss_names_the_most_of_each(self):
        sizes = [{"cpu": 4, "memory_gib": 30}, {"cpu": 8, "memory_gib": 16}]
        cap = local_capabilities(adapter="ecs-fargate", family="serverless", max_cpu=8,
                                 max_memory_gib=40, task_sizes=sizes)
        found = check_spec(cap, spec(resources=Resources(cpu=1, memory_gib=35)))
        assert found == ["no ecs-fargate task size has 1 CPU and 35 GiB of memory; its sizes "
                         "go up to 8 CPU and up to 30 GiB"]

    def test_task_sizes_are_within_the_maxima_and_never_empty(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="at least 1 item"):
            local_capabilities(task_sizes=[])
        with pytest.raises(ValidationError, match="beyond max_cpu or max_memory_gib"):
            local_capabilities(max_cpu=2, task_sizes=[{"cpu": 4, "memory_gib": 8}])

    def test_unknown_scratch_placement_rejects_requests_with_scratch(self):
        cap = local_capabilities(scratch_in_memory=None)
        assert "has not declared whether scratch is held in memory" in " | ".join(
            check_spec(cap, spec()))
        assert check_spec(cap, spec(resources=Resources(scratch_gib=0))) == []

    def test_a_runtime_cap_of_unknown_length_rejects(self):
        cap = local_capabilities(has_runtime_cap=True, max_task_seconds=None)
        assert any("caps task runtime but has not declared how long" in f
                   for f in check_spec(cap, spec()))

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


def attempt_manifest(actor: Actor) -> AttemptManifest:
    now = datetime(2026, 11, 20, tzinfo=timezone.utc)
    return AttemptManifest(
        run_manifest_sha256="a" * 64, status="completed",
        artifacts=[ArtifactEntry(path="run.json", sha256="a" * 64, size=10,
                                 kind="run_manifest", format="json"),
                   ArtifactEntry(path="summary.json", sha256="b" * 64, size=20,
                                 kind="summary", format="json")],
        provenance=Provenance(
            run_id="run_1", job_id="job_1", attempt_id="att_1", attempt_number=1,
            revision_id="rev_1", revision_manifest_sha256="c" * 64,
            composition_sha256="d" * 64, inputs={"model/model.inp": "e" * 64},
            workflow="swmm.simulation", workflow_version="1", parameters={}, threads=1,
            runtime=RuntimeVersions(platform="0", python="3"), actor=actor,
            started_at=now, finished_at=now))


class TestProvenance:
    def test_a_person_alone_is_the_only_agent(self):
        prov = attempt_manifest(Actor(human_subject="alice")).to_prov()
        assert list(prov["agent"]) == ["hcp:human/alice"]
        assert "actedOnBehalfOf" not in prov

    def test_an_agent_session_acts_on_behalf_of_the_person(self):
        actor = Actor(human_subject="alice", agent_session_id="ags_1", agent_client="openswmm.mcp",
                      model="m", tool_call_id="call_7")
        prov = attempt_manifest(actor).to_prov()
        session = "hcp:agent-session/ags_1"
        assert prov["agent"][session] == {"hcp:client": "openswmm.mcp", "hcp:model": "m",
                                          "hcp:toolCall": "call_7"}
        assert prov["actedOnBehalfOf"]["d0"] == {
            "prov:delegate": session, "prov:responsible": "hcp:human/alice",
            "prov:activity": "hcp:attempt/att_1"}
        assert {w["prov:agent"] for w in prov["wasAssociatedWith"].values()} == {
            session, "hcp:human/alice"}

    def test_every_artifact_is_generated_by_the_attempt(self):
        prov = attempt_manifest(Actor(human_subject="alice")).to_prov()
        generated = {g["prov:entity"] for g in prov["wasGeneratedBy"].values()}
        assert generated == {"hcp:artifact/" + "a" * 64, "hcp:artifact/" + "b" * 64}
        assert all(g["prov:activity"] == "hcp:attempt/att_1"
                   for g in prov["wasGeneratedBy"].values())
        assert prov["entity"]["hcp:artifact/" + "b" * 64] == {"hcp:path": "summary.json",
                                                              "hcp:kind": "summary"}
        assert "hcp:input/" + "e" * 64 in prov["entity"]  # the inputs are still there


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
