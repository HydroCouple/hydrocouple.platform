"""Estimates, submissions, approvals, cancellation, status and results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..contracts.agent import GrantMode
from ..contracts.capabilities import AdapterCapabilities, check_spec
from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.jobs import (
    Actor,
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
from ..contracts.manifests import sha256_of
from ..contracts.requests import Budget, Estimate, JobView, RunAccepted, RunRequest
from ..contracts.states import JOB_HELD, JOB_TERMINAL, JobState
from ..db.models import (
    AgentSession,
    Approval,
    Artifact,
    Attempt,
    ComputeTarget,
    Job,
    Outbox,
    PlatformSession,
    Project,
    Revision,
    Run,
    new_id,
)
from ..workflows import Workflow, get_workflow
from .auth import Principal
from .base import ServiceBase, human_actor

PROFILES: dict[str, dict[str, float]] = {
    "cpu-small": {"cpu": 1, "memory_gib": 2, "scratch_gib": 4},
    "cpu-standard": {"cpu": 4, "memory_gib": 8, "scratch_gib": 20},
    "memory-large": {"cpu": 8, "memory_gib": 64, "scratch_gib": 50},
}
ACTIVE_STATES = [s.value for s in JobState if s not in JOB_TERMINAL]
DEFAULT_RETRY_BUDGET = 2


def run_prefix(organization_id: str, project_id: str, run_id: str) -> str:
    return f"runs/{organization_id}/{project_id}/{run_id}/"


@dataclass
class _Checked:
    project: Project
    revision: Revision
    target: ComputeTarget
    workflow: Workflow
    parameters: dict[str, Any]
    resources: Resources
    findings: list[str]
    request_hash: str


class RunServices(ServiceBase):
    # -- shared validation -------------------------------------------------------

    def _check(self, s: Session, principal: Principal, req: RunRequest, minimum: str) -> _Checked:
        project = self.owned(s, Project, req.project_id, principal, minimum)
        revision = self.owned(s, Revision, req.revision_id, principal, "viewer")
        target = self.owned(s, ComputeTarget, req.target_id, principal, "viewer")
        if revision.project_id != project.id or target.organization_id != project.organization_id:
            raise PlatformError(ErrorCode.NOT_FOUND, "the revision or target was not found")
        if req.session_id is not None:
            ses = self.owned(s, PlatformSession, req.session_id, principal, "viewer")
            if ses.project_id != project.id:
                raise PlatformError(ErrorCode.NOT_FOUND, f"session {req.session_id} was not found")
        workflow = get_workflow(req.workflow, req.workflow_version)
        parameters = workflow.validate_parameters(req.parameters)

        findings: list[str] = []
        if revision.state != "READY":
            findings.append(f"revision {revision.id} is {revision.state}, not READY")
        if revision.target_id != target.id:
            findings.append("the revision is stored on another target; data is never moved "
                            "between targets implicitly")
        if target.revoked_at is not None:
            findings.append("the target was revoked")
        components = set(revision.manifest.get("components", []))
        unlisted = sorted(components - workflow.allowed_components)
        if unlisted:
            findings.append(f"components not approved for {workflow.name}@{workflow.version}: "
                            + ", ".join(unlisted))
        profile_name = req.profile or workflow.default_profile
        if profile_name not in PROFILES:
            raise PlatformError(ErrorCode.VALIDATION_ERROR, f"unknown profile {profile_name!r}")
        resources = Resources(job_class=workflow.job_class, profile=profile_name,
                              **PROFILES[profile_name])

        if target.capabilities is None:
            findings.append("the target's agent has not reported its capabilities yet; "
                            "start the agent and try again")
        else:
            cap = AdapterCapabilities.model_validate(target.capabilities)
            probe = self._spec(
                "job_probe", "run_probe", project, revision, target, workflow, parameters,
                resources, req, Actor(human_subject=principal.subject),
            )
            findings.extend(check_spec(cap, probe))

        request_hash = sha256_of({
            "project_id": project.id, "session_id": req.session_id, "revision_id": revision.id,
            "manifest_sha256": revision.manifest_sha256, "workflow": workflow.name,
            "workflow_version": workflow.version, "parameters": parameters,
            "target_id": target.id, "profile": profile_name,
            "retry_budget": DEFAULT_RETRY_BUDGET if req.retry_budget is None else req.retry_budget,
            "estimated_runtime_seconds": req.estimated_runtime_seconds,
            "agent_session_id": principal.agent_session_id,
        })
        return _Checked(project, revision, target, workflow, parameters, resources, findings,
                        request_hash)

    @staticmethod
    def _run_findings(checked: _Checked) -> list[str]:
        if checked.workflow.job_class == JobClass.EVALUATOR_POOL:
            return [f"{checked.workflow.name}@{checked.workflow.version} evaluates study "
                    "candidates; create a study to use it"]
        return []

    def _spec(self, job_id, run_id, project, revision, target, workflow, parameters, resources,
              req: RunRequest, actor: Actor) -> JobSpec:
        threads = int(parameters.get("threads", 1))
        return JobSpec(
            job_id=job_id, run_id=run_id, organization_id=project.organization_id,
            project_id=project.id, actor=actor,
            workload=Workload(workflow=workflow.name, workflow_version=workflow.version,
                              parameters=parameters),
            runtime=Runtime(image=workflow.images.get("default"), threads=threads),
            inputs=Inputs(
                revision_id=revision.id, manifest_sha256=revision.manifest_sha256,
                storage_prefix=revision.storage_prefix,
                composition=revision.manifest.get("composition", "composition.json"),
                files=[{"path": f["path"], "sha256": f["sha256"], "size": f["size"]}
                       for f in revision.manifest["files"]],
            ),
            resources=resources,
            placement=Placement(target_id=target.id),
            limits=Limits(
                retry_budget=DEFAULT_RETRY_BUDGET if req.retry_budget is None else req.retry_budget,
                estimated_runtime_seconds=req.estimated_runtime_seconds,
            ),
            outputs=Outputs(
                storage_prefix=run_prefix(project.organization_id, project.id, run_id),
                required_artifacts=list(workflow.required_artifacts),
                egress_policy=target.egress_policy,
            ),
        )

    def _budget_reasons(self, s: Session, session: AgentSession, checked: _Checked) -> list[str]:
        budget = Budget.model_validate(session.budget)
        reasons: list[str] = []
        if budget.max_runs is not None and session.runs_submitted >= budget.max_runs:
            reasons.append(f"the agent session has used its {budget.max_runs} runs")
        if budget.max_concurrent_attempts is not None:
            active = s.scalar(
                select(func.count()).select_from(Job).join(Run, Run.id == Job.run_id)
                .where(Run.agent_session_id == session.id, Job.state.in_(ACTIVE_STATES))
            ) or 0
            if active >= budget.max_concurrent_attempts:
                reasons.append(f"{active} jobs are already active; the session allows "
                               f"{budget.max_concurrent_attempts}")
        if budget.allowed_targets is not None and checked.target.id not in budget.allowed_targets:
            reasons.append(f"target {checked.target.id} is outside the session's allowed targets")
        if (budget.allowed_job_classes is not None
                and checked.resources.job_class not in budget.allowed_job_classes):
            reasons.append(f"job class {checked.resources.job_class.value} is not allowed")
        if budget.max_estimated_spend is not None:
            reasons.append("the session caps spend but this target has no price source, so the "
                           "cost of this run is unknown")
        return reasons

    def _agent_session(self, s: Session, principal: Principal, agent_session_id: str,
                       project: Project, lock: bool) -> AgentSession:
        session = self.owned(s, AgentSession, agent_session_id, principal, "viewer", lock=lock)
        if session.project_id != project.id:
            raise PlatformError(ErrorCode.FORBIDDEN, "the agent session belongs to another project")
        if session.revoked_at is not None or session.expires_at <= self.now():
            raise PlatformError(ErrorCode.FORBIDDEN, "the agent session expired or was revoked",
                                next_action="Ask the user to start a new agent session.")
        return session

    # -- estimate ------------------------------------------------------------------

    def estimate(self, principal: Principal, req: RunRequest) -> Estimate:
        with self.tx() as s:
            checked = self._check(s, principal, req, "viewer")
            checked.findings.extend(self._run_findings(checked))
            reasons: list[str] = []
            if principal.agent_session_id:
                reasons = self._budget_reasons(s, self._agent_session(
                    s, principal, principal.agent_session_id, checked.project, False), checked)
            return Estimate(
                ok=not checked.findings, findings=checked.findings,
                job_class=checked.resources.job_class, profile=checked.resources.profile,
                requires_approval=bool(reasons), approval_reasons=reasons,
            )

    # -- submit ----------------------------------------------------------------------

    def submit_run(self, principal: Principal, req: RunRequest,
                   idempotency_key: str) -> RunAccepted:
        """Accept a run. An AI agent's budget applies whenever it is the caller: the
        session comes from its credential, never from an optional field."""
        if not 8 <= len(idempotency_key) <= 200:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "the idempotency key must be 8 to 200 characters")
        try:
            return self._submit(principal, req, idempotency_key)
        except IntegrityError:
            # A concurrent request with the same key won the insert; answer as it did.
            with self.tx() as s:
                checked = self._check(s, principal, req, "editor")
                existing = self._existing(s, checked.project.organization_id, idempotency_key,
                                          checked.request_hash)
                if existing is None:
                    raise
                return existing

    def _existing(self, s: Session, org_id: str, key: str, request_hash: str) -> RunAccepted | None:
        run = s.scalar(select(Run).where(Run.organization_id == org_id,
                                         Run.idempotency_key == key))
        if run is None:
            return None
        if run.request_hash != request_hash:
            raise PlatformError(
                ErrorCode.CONFLICT,
                "this idempotency key was already used for a different request",
                next_action="Use a new idempotency key for a different submission.",
            )
        job = s.scalar(select(Job).where(Job.run_id == run.id))
        approval = s.scalar(select(Approval).where(Approval.job_id == job.id))
        return self._accepted(run, job, approval, None)

    def _submit(self, principal, req, key) -> RunAccepted:
        with self.tx() as s:
            checked = self._check(s, principal, req, "editor")
            org_id = checked.project.organization_id
            existing = self._existing(s, org_id, key, checked.request_hash)
            if existing is not None:
                return existing
            checked.findings.extend(self._run_findings(checked))
            if checked.findings:
                raise PlatformError(
                    ErrorCode.CAPABILITY_UNSUPPORTED if checked.target.capabilities
                    else ErrorCode.NOT_READY,
                    "the run cannot be accepted: " + "; ".join(checked.findings),
                    details={"findings": checked.findings},
                    next_action="Call platform_estimate to see every finding before submitting.",
                )

            session = None
            reasons: list[str] = []
            actor = Actor(human_subject=principal.subject, tool_call_id=req.tool_call_id)
            if principal.agent_session_id:
                session = self._agent_session(s, principal, principal.agent_session_id,
                                              checked.project, lock=True)
                reasons = self._budget_reasons(s, session, checked)
                actor = Actor(
                    human_subject=principal.subject, agent_session_id=session.id,
                    agent_client=session.client_name, agent_client_version=session.client_version,
                    model=session.model, conversation_ref=session.conversation_ref,
                    tool_call_id=req.tool_call_id,
                )

            run_id, job_id = new_id("run"), new_id("job")
            spec = self._spec(job_id, run_id, checked.project, checked.revision, checked.target,
                              checked.workflow, checked.parameters, checked.resources, req, actor)
            run = Run(
                id=run_id, organization_id=org_id, project_id=checked.project.id,
                session_id=req.session_id, revision_id=checked.revision.id,
                target_id=checked.target.id, workflow=checked.workflow.name,
                workflow_version=checked.workflow.version, parameters=checked.parameters,
                job_class=checked.resources.job_class.value, idempotency_key=key,
                request_hash=checked.request_hash, actor=actor.model_dump(mode="json"),
                agent_session_id=session.id if session else None, created_by=principal.user_id,
            )
            state = JobState.PENDING_APPROVAL if reasons else JobState.ACCEPTED
            job = Job(id=job_id, run_id=run_id, organization_id=org_id,
                      target_id=checked.target.id, state=state.value,
                      spec=spec.model_dump(mode="json"), retry_budget=spec.limits.retry_budget)
            s.add(run)
            s.flush()
            s.add(job)
            s.flush()
            approval = None
            if reasons:
                approval = Approval(
                    id=new_id("apr"), organization_id=org_id, project_id=checked.project.id,
                    run_id=run_id, job_id=job_id,
                    agent_session_id=session.id if session else None, reasons=reasons,
                    expires_at=self.now() + timedelta(seconds=self.settings.approval_ttl_seconds),
                )
                s.add(approval)
            else:
                s.add(Outbox(topic="job.accepted", payload={"job_id": job_id}))
            if session is not None:
                session.runs_submitted += 1
            self.audit(s, org_id, actor.model_dump(mode="json"), "run.submit", "run", run_id,
                       {"job_id": job_id, "state": state.value, "approval_reasons": reasons})
            s.flush()
            return self._accepted(run, job, approval, session)

    def _accepted(self, run: Run, job: Job, approval: Approval | None,
                  session: AgentSession | None) -> RunAccepted:
        remaining = None
        if session is not None:
            budget = Budget.model_validate(session.budget)
            if budget.max_runs is not None:
                remaining = {"runs": max(budget.max_runs - session.runs_submitted, 0)}
        pending = job.state == JobState.PENDING_APPROVAL.value
        return RunAccepted(
            platform_session_id=run.session_id, run_id=run.id, job_id=job.id, status=job.state,
            job_class=run.job_class, model_revision_id=run.revision_id,
            compute_target_id=run.target_id, approval_id=approval.id if approval else None,
            budget_remaining=remaining,
            next_action=(f"A project editor must resolve approval {approval.id}; then query "
                         f"platform_job with {job.id}") if pending and approval
            else f"Query platform_job with {job.id}",
        )

    # -- approvals ------------------------------------------------------------------

    def resolve_approval(self, principal: Principal, approval_id: str, approve: bool,
                         note: str | None = None) -> dict:
        self.human_only(principal, "resolve approvals")
        with self.tx() as s:
            approval = self.owned(s, Approval, approval_id, principal, "editor", lock=True)
            if approval.state != "PENDING":
                raise PlatformError(ErrorCode.INVALID_TRANSITION,
                                    f"approval {approval.id} is already {approval.state}")
            if approval.expires_at <= self.now():
                raise PlatformError(ErrorCode.INVALID_TRANSITION,
                                    f"approval {approval.id} expired")
            approval.state = "APPROVED" if approve else "REJECTED"
            approval.resolved_by, approval.resolved_at, approval.note = (
                principal.user_id, self.now(), note)
            if approval.study_id is not None:
                study_state = self.resolve_study_approval(  # type: ignore[attr-defined]
                    s, approval, approve, note)
                self.audit(s, approval.organization_id, human_actor(principal),
                           "approval." + approval.state.lower(), "approval", approval.id,
                           {"study_id": approval.study_id, "note": note})
                return {"approval_id": approval.id, "state": approval.state,
                        "study_state": study_state}
            job = s.get(Job, approval.job_id, with_for_update=True)
            if job.state == JobState.PENDING_APPROVAL.value:
                if approve:
                    self._set_state(job, JobState.ACCEPTED)
                    s.add(Outbox(topic="job.accepted", payload={"job_id": job.id}))
                else:
                    self._set_state(job, JobState.REJECTED, failure={"reason": "approval rejected",
                                                                       "note": note})
            self.audit(s, approval.organization_id, human_actor(principal),
                       "approval." + approval.state.lower(), "approval", approval.id,
                       {"job_id": job.id, "note": note})
            return {"approval_id": approval.id, "state": approval.state, "job_state": job.state}

    def list_approvals(self, principal: Principal, project_id: str) -> list[dict]:
        with self.tx() as s:
            self.owned(s, Project, project_id, principal, "viewer")
            rows = s.scalars(select(Approval).where(
                Approval.project_id == project_id, Approval.state == "PENDING"))
            return [{"approval_id": a.id, "run_id": a.run_id, "job_id": a.job_id,
                     "study_id": a.study_id, "reasons": a.reasons,
                     "expires_at": a.expires_at.isoformat()} for a in rows]

    # -- cancellation -----------------------------------------------------------------

    def cancel_job(self, principal: Principal, job_id: str) -> dict:
        with self.tx() as s:
            job = self.owned(s, Job, job_id, principal, "editor", lock=True)
            state = JobState(job.state)
            if state in JOB_TERMINAL:
                return {"job_id": job.id, "state": job.state, "cancel_requested": False}
            if state in JOB_HELD:
                job.cancel_requested = True
                job.updated_at = self.now()
            else:
                self._set_state(job, JobState.CANCELLED, failure={"reason": "cancelled by user"})
            self.audit(s, job.organization_id, human_actor(principal), "job.cancel", "job", job.id,
                       {"state_at_request": state.value})
            return {"job_id": job.id, "state": job.state,
                    "cancel_requested": job.cancel_requested}

    # -- views -------------------------------------------------------------------------

    def get_job(self, principal: Principal, job_id: str) -> JobView:
        with self.tx() as s:
            job = self.owned(s, Job, job_id, principal, "viewer")
            attempts = s.scalars(select(Attempt).where(Attempt.job_id == job.id)
                                 .order_by(Attempt.number))
            return JobView(
                job_id=job.id, run_id=job.run_id, state=job.state,
                cancel_requested=job.cancel_requested, agent_unreachable=job.agent_unreachable,
                attempts_used=job.attempts_used, retry_budget=job.retry_budget,
                target_id=job.target_id, created_at=job.created_at, updated_at=job.updated_at,
                failure=job.failure, progress=job.progress,
                attempts=[{"attempt_id": a.id, "number": a.number, "state": a.state,
                           "failure_class": a.failure_class, "exit_code": a.exit_code,
                           "started_at": a.started_at.isoformat(),
                           "finished_at": a.finished_at.isoformat() if a.finished_at else None}
                          for a in attempts],
            )

    def list_jobs(self, principal: Principal, project_id: str, state: str | None = None,
                  limit: int = 50, before: str | None = None) -> dict:
        limit = max(1, min(limit, 200))
        with self.tx() as s:
            self.owned(s, Project, project_id, principal, "viewer")
            q = (select(Job).join(Run, Run.id == Job.run_id)
                 .where(Run.project_id == project_id))
            if state:
                q = q.where(Job.state == state)
            if before:
                q = q.where(Job.id < before)
            rows = list(s.scalars(q.order_by(Job.id.desc()).limit(limit + 1)))
            page = rows[:limit]
            return {"jobs": [{"job_id": j.id, "run_id": j.run_id, "state": j.state,
                              "created_at": j.created_at.isoformat()} for j in page],
                    "next_before": page[-1].id if len(rows) > limit else None}

    def get_results(self, principal: Principal, run_id: str) -> dict:
        with self.tx() as s:
            run = self.owned(s, Run, run_id, principal, "viewer")
            if run.canonical_attempt_id is None:
                job = s.scalar(select(Job).where(Job.run_id == run.id))
                raise PlatformError(ErrorCode.NOT_READY,
                                    f"run {run.id} has no published result (job is {job.state})",
                                    retryable=job.state not in {x.value for x in JOB_TERMINAL},
                                    next_action=f"Query platform_job with {job.id}")
            attempt = s.get(Attempt, run.canonical_attempt_id)
            artifacts = s.scalars(select(Artifact).where(Artifact.attempt_id == attempt.id)
                                  .order_by(Artifact.path))
            manifest = attempt.manifest or {}
            return {
                "run_id": run.id,
                "provenance": manifest.get("provenance"),
                "status": manifest.get("status"),
                "quality": manifest.get("quality"),
                "summary": run.summary,
                "attempt_manifest_sha256": attempt.manifest_sha256,
                "artifacts": [{"path": a.path, "kind": a.kind, "format": a.format,
                               "sha256": a.sha256, "size": a.size} for a in artifacts],
                "published_at": run.published_at.isoformat() if run.published_at else None,
            }

    def request_downloads(self, principal: Principal, run_id: str, paths: list[str]) -> dict:
        with self.tx() as s:
            run = self.owned(s, Run, run_id, principal, "viewer")
            if run.canonical_attempt_id is None:
                raise PlatformError(ErrorCode.NOT_READY, f"run {run.id} is not published")
            arts = {a.path: a for a in s.scalars(select(Artifact).where(
                Artifact.attempt_id == run.canonical_attempt_id))}
            missing = [p for p in paths if p not in arts]
            if missing:
                raise PlatformError(ErrorCode.NOT_FOUND, "unknown artifacts: " + ", ".join(missing))
            grant = self._grant_request(
                s, run.organization_id, run.target_id, principal, GrantMode.DOWNLOAD,
                [{"key": arts[p].storage_key, "sha256": arts[p].sha256, "size": arts[p].size}
                 for p in paths], run_id=run.id)
            return {"grant_request_id": grant, "keys": {p: arts[p].storage_key for p in paths}}
