"""Studies: batches of candidates evaluated on fenced evaluator pools.

Locks follow the platform order, job then attempt then study then chunk, so the
agent routes, the reconciler and the client routes cannot deadlock. A path that
holds a study lock touches other jobs only with SKIP LOCKED, or after it
commits.

Rows are kept per study, batch and chunk, never per evaluation: an evaluator
writes the per-evaluation records as an append-only object per chunk, and the
chunk row records that object's key and checksum.
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.jobs import Actor, FailureClass, JobClass, JobSpec
from ..contracts.manifests import check_safe_path, sha256_of
from ..contracts.requests import Budget, RunRequest
from ..contracts.states import JOB_TERMINAL, JobState
from ..contracts.studies import (
    STUDY_TERMINAL,
    BatchState,
    BatchSubmit,
    BatchView,
    CompletionAck,
    EvaluationChunk,
    EvaluationCompletion,
    EvaluationLeaseRequest,
    EvaluationLeaseResponse,
    EvaluationRecord,
    FailurePolicy,
    Objective,
    StudyCreate,
    StudyState,
    StudyView,
)
from ..db.models import (
    AgentSession,
    Approval,
    Attempt,
    Job,
    Outbox,
    Run,
    Study,
    StudyBatch,
    new_id,
)
from ..db.models import EvaluationChunk as ChunkRow
from .auth import Principal
from .base import ServiceBase, human_actor

# A batch is split into about two chunks per pool task, at most this many candidates each.
MAX_CHUNK = 32
# A chunk delivered this many times without a result fails its candidates.
MAX_DELIVERIES = 3
# Task jobs that fail, one after another, without completing a chunk before the study fails.
MAX_TASK_FAILURES = 3
TERMINAL_JOB_STATES = [s.value for s in JOB_TERMINAL]


def chunk_ranges(size: int, pool_size: int) -> list[tuple[int, int]]:
    per = max(1, min(MAX_CHUNK, math.ceil(size / (2 * pool_size))))
    return [(start, min(start + per, size)) for start in range(0, size, per)]


def pareto_indices(points: list[tuple[int, list[float]]]) -> list[int]:
    """The Pareto set for minimization, as ``openswmm_gymnasium.optimize.assemble_result``.

    A point is dominated by another that is no worse in every objective and
    better in at least one (``openswmm_gymnasium.scoring.pareto_front``). Like
    ``assemble_result``, the set then holds every point ``numpy.allclose`` to a
    front point, so duplicates and near-duplicates of the front are kept.
    Returns the indices in order.
    """
    def dominates(a: list[float], b: list[float]) -> bool:
        pairs = list(zip(a, b, strict=True))
        return all(x <= y for x, y in pairs) and any(x < y for x, y in pairs)

    front: list[tuple[int, list[float]]] = []
    for index, costs in points:
        if any(dominates(other, costs) for _, other in front):
            continue
        front = [(i, c) for i, c in front if not dominates(costs, c)] + [(index, costs)]

    def allclose(a: list[float], b: list[float]) -> bool:  # numpy's defaults
        return all(abs(x - y) <= 1e-8 + 1e-5 * abs(y) for x, y in zip(a, b, strict=True))

    return [index for index, costs in points
            if any(allclose(costs, f) for _, f in front)]


class StudyServices(ServiceBase):
    # -- creation ----------------------------------------------------------------

    def _run_request(self, req: StudyCreate) -> RunRequest:
        return RunRequest(
            project_id=req.project_id, session_id=req.session_id, revision_id=req.revision_id,
            workflow=req.workflow, workflow_version=req.workflow_version,
            parameters=req.parameters, target_id=req.target_id, profile=req.profile,
            tool_call_id=req.tool_call_id,
        )

    def _check_study(self, s: Session, principal: Principal, req: StudyCreate, minimum: str):
        checked = self._check(s, principal, self._run_request(req), minimum)  # type: ignore[attr-defined]
        if checked.workflow.job_class != JobClass.EVALUATOR_POOL:
            raise PlatformError(
                ErrorCode.VALIDATION_ERROR,
                f"{checked.workflow.name}@{checked.workflow.version} is not an evaluator "
                "workflow; a study needs one", next_action="Call platform_targets for the "
                "approved evaluator workflows.")
        checked.findings.extend(self._evaluator_findings(checked.parameters, checked.revision))
        request_hash = sha256_of({
            "run": checked.request_hash,
            "dimensions": [d.model_dump() for d in req.dimensions],
            "objectives": [o.model_dump() for o in req.objectives], "seed": req.seed,
            "on_failure": req.on_failure.value, "failure_penalty": req.failure_penalty,
            "max_evaluations": req.max_evaluations, "pool_size": req.pool_size,
        })
        return checked, request_hash

    @staticmethod
    def _evaluator_findings(parameters: dict[str, Any], revision) -> list[str]:
        """The evaluator's model must be a file of the revision."""
        inp = (parameters.get("env_config") or {}).get("inp_path")
        if not isinstance(inp, str):
            return ["env_config.inp_path must name the revision's model file"]
        try:
            check_safe_path(inp)
        except ValueError as exc:
            return [f"env_config.inp_path: {exc}"]
        files = {f["path"] for f in revision.manifest.get("files", [])}
        if inp not in files:
            return [f"env_config.inp_path {inp!r} is not a file of revision {revision.id}"]
        return []

    def _study_budget_reasons(self, s: Session, session: AgentSession, checked,
                              req: StudyCreate) -> list[str]:
        reasons = self._budget_reasons(  # type: ignore[attr-defined]
            s, session, checked, slots=req.pool_size)
        budget = Budget.model_validate(session.budget)
        if budget.max_evaluations is not None:
            reserved = s.scalar(
                select(func.coalesce(func.sum(Study.max_evaluations), 0))
                .where(Study.agent_session_id == session.id,
                       Study.state != StudyState.REJECTED.value)) or 0
            if reserved + req.max_evaluations > budget.max_evaluations:
                reasons.append(
                    f"the study asks for {req.max_evaluations} evaluations; the session has "
                    f"{max(budget.max_evaluations - reserved, 0)} of {budget.max_evaluations} left")
        return reasons

    def create_study(self, principal: Principal, req: StudyCreate, idempotency_key: str
                     ) -> StudyView:
        if not 8 <= len(idempotency_key) <= 200:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "the idempotency key must be 8 to 200 characters")
        try:
            return self._create_study(principal, req, idempotency_key)
        except IntegrityError:
            with self.tx() as s:  # a concurrent request with the same key won the insert
                checked, request_hash = self._check_study(s, principal, req, "editor")
                existing = self._existing_study(s, checked.project.organization_id,
                                                idempotency_key, request_hash)
                if existing is None:
                    raise
                return self._view(s, existing)

    @staticmethod
    def _existing_study(s: Session, org_id: str, key: str, request_hash: str) -> Study | None:
        study = s.scalar(select(Study).where(Study.organization_id == org_id,
                                             Study.idempotency_key == key))
        if study is not None and study.request_hash != request_hash:
            raise PlatformError(ErrorCode.CONFLICT,
                                "this idempotency key was already used for a different study",
                                next_action="Use a new idempotency key for a different study.")
        return study

    def _create_study(self, principal: Principal, req: StudyCreate, key: str) -> StudyView:
        with self.tx() as s:
            checked, request_hash = self._check_study(s, principal, req, "editor")
            org_id = checked.project.organization_id
            existing = self._existing_study(s, org_id, key, request_hash)
            if existing is not None:
                return self._view(s, existing)
            if checked.findings:
                raise PlatformError(
                    ErrorCode.CAPABILITY_UNSUPPORTED if checked.target.capabilities
                    else ErrorCode.NOT_READY,
                    "the study cannot be accepted: " + "; ".join(checked.findings),
                    details={"findings": checked.findings},
                    next_action="Fix the findings, then create the study again.")

            session, reasons = None, []
            actor = Actor(human_subject=principal.subject, tool_call_id=req.tool_call_id)
            if principal.agent_session_id:
                session = self._agent_session(  # type: ignore[attr-defined]
                    s, principal, principal.agent_session_id, checked.project, lock=True)
                reasons = self._study_budget_reasons(s, session, checked, req)
                actor = Actor(
                    human_subject=principal.subject, agent_session_id=session.id,
                    agent_client=session.client_name, agent_client_version=session.client_version,
                    model=session.model, conversation_ref=session.conversation_ref,
                    tool_call_id=req.tool_call_id)

            study_id, run_id = new_id("std"), new_id("run")
            parameters = {**checked.parameters, "study": {
                "study_id": study_id, "seed": req.seed,
                "dimensions": [d.model_dump() for d in req.dimensions],
                "objectives": [o.model_dump() for o in req.objectives],
            }}
            template = self._spec(  # type: ignore[attr-defined]
                "job_template", run_id, checked.project, checked.revision, checked.target,
                checked.workflow, parameters, checked.resources, self._run_request(req), actor)
            run = Run(
                id=run_id, organization_id=org_id, project_id=checked.project.id,
                session_id=req.session_id, revision_id=checked.revision.id,
                target_id=checked.target.id, workflow=checked.workflow.name,
                workflow_version=checked.workflow.version, parameters=checked.parameters,
                job_class=JobClass.EVALUATOR_POOL.value, idempotency_key=f"study/{study_id}",
                request_hash=request_hash, actor=actor.model_dump(mode="json"),
                agent_session_id=session.id if session else None, created_by=principal.user_id)
            state = StudyState.PENDING_APPROVAL if reasons else StudyState.OPEN
            study = Study(
                id=study_id, organization_id=org_id, project_id=checked.project.id,
                session_id=req.session_id, revision_id=checked.revision.id,
                target_id=checked.target.id, run_id=run_id, state=state.value,
                definition={
                    "workflow": checked.workflow.name,
                    "workflow_version": checked.workflow.version,
                    "dimensions": [d.model_dump() for d in req.dimensions],
                    "objectives": [o.model_dump() for o in req.objectives],
                    "seed": req.seed, "on_failure": req.on_failure.value,
                    "failure_penalty": req.failure_penalty,
                    "evaluation_timeout_seconds": checked.parameters.get(
                        "evaluation_timeout_seconds", 600),
                    "idle_timeout_seconds": checked.parameters.get("idle_timeout_seconds", 600),
                    "task_spec": template.model_dump(mode="json"),
                },
                max_evaluations=req.max_evaluations, pool_size=req.pool_size,
                idempotency_key=key, request_hash=request_hash,
                actor=actor.model_dump(mode="json"),
                agent_session_id=session.id if session else None, created_by=principal.user_id)
            s.add(run)
            s.flush()
            s.add(study)
            s.flush()
            if reasons:
                s.add(Approval(
                    id=new_id("apr"), organization_id=org_id, project_id=checked.project.id,
                    run_id=run_id, job_id=None, study_id=study_id,
                    agent_session_id=session.id if session else None, reasons=reasons,
                    expires_at=self.now() + timedelta(seconds=self.settings.approval_ttl_seconds)))
            if session is not None:
                session.runs_submitted += 1
            self.audit(s, org_id, actor.model_dump(mode="json"), "study.create", "study", study_id,
                       {"state": state.value, "approval_reasons": reasons,
                        "max_evaluations": req.max_evaluations, "pool_size": req.pool_size})
            s.flush()
            return self._view(s, study)

    # -- batches ---------------------------------------------------------------------

    def submit_batch(self, principal: Principal, study_id: str, body: BatchSubmit,
                     idempotency_key: str) -> BatchView:
        if not 8 <= len(idempotency_key) <= 200:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "the idempotency key must be 8 to 200 characters")
        request_hash = sha256_of({"candidates": body.candidates})
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "editor", lock=True)
            if principal.agent_session_id and study.agent_session_id != principal.agent_session_id:
                # Its budget was checked against the studies it created, and only those.
                raise PlatformError(
                    ErrorCode.FORBIDDEN, "an AI agent session submits batches only to studies "
                    "it created", next_action="Create a study under this agent session.")
            existing = s.scalar(select(StudyBatch).where(
                StudyBatch.study_id == study.id, StudyBatch.idempotency_key == idempotency_key))
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise PlatformError(ErrorCode.CONFLICT, "this idempotency key was already "
                                        "used for a different batch")
                return self._batch_view(s, study, existing)
            if StudyState(study.state) not in (StudyState.OPEN, StudyState.PENDING_APPROVAL):
                raise PlatformError(ErrorCode.INVALID_TRANSITION,
                                    f"study {study.id} is {study.state} and takes no batches",
                                    details={"failure": study.failure})
            self._check_candidates(study, body.candidates)
            left = study.max_evaluations - study.evaluations_submitted
            if len(body.candidates) > left:
                raise PlatformError(
                    ErrorCode.VALIDATION_ERROR,
                    f"the batch has {len(body.candidates)} candidates; the study's evaluation "
                    f"budget allows {left} more",
                    next_action="Submit a smaller batch or close the study.")
            batch = StudyBatch(
                id=new_id("bat"), study_id=study.id, organization_id=study.organization_id,
                sequence=study.batches_submitted + 1, offset=study.evaluations_submitted,
                size=len(body.candidates), idempotency_key=idempotency_key,
                request_hash=request_hash, candidates=body.candidates,
                state=BatchState.QUEUED.value)
            s.add(batch)
            s.flush()
            for start, stop in chunk_ranges(batch.size, study.pool_size):
                s.add(ChunkRow(id=new_id("chk"), study_id=study.id, batch_id=batch.id,
                               organization_id=study.organization_id,
                               batch_sequence=batch.sequence, start=start, stop=stop))
            study.batches_submitted += 1
            study.evaluations_submitted += batch.size
            study.updated_at = self.now()
            s.flush()
            self._top_up(s, study)
            return self._batch_view(s, study, batch)

    @staticmethod
    def _check_candidates(study: Study, candidates: list[list[float]]) -> None:
        dims = study.definition["dimensions"]
        for i, vector in enumerate(candidates):
            if len(vector) != len(dims):
                raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                    f"candidate {i} has {len(vector)} values; the study has "
                                    f"{len(dims)} dimensions")
            for value, dim in zip(vector, dims, strict=True):
                slack = 1e-9 * max(1.0, abs(dim["low"]), abs(dim["high"]))
                if not math.isfinite(value) or not \
                        dim["low"] - slack <= value <= dim["high"] + slack:
                    raise PlatformError(
                        ErrorCode.VALIDATION_ERROR,
                        f"candidate {i}: {dim['label']} = {value} is outside "
                        f"[{dim['low']}, {dim['high']}]")

    def get_batch(self, principal: Principal, study_id: str, batch_id: str) -> BatchView:
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "viewer")
            batch = s.get(StudyBatch, batch_id)
            if batch is None or batch.study_id != study.id:
                raise PlatformError(ErrorCode.NOT_FOUND, f"batch {batch_id} was not found")
            return self._batch_view(s, study, batch)

    def _batch_view(self, s: Session, study: Study, batch: StudyBatch) -> BatchView:
        results = None
        if batch.results is not None:
            results = [EvaluationRecord.model_validate(r) for r in batch.results]
        nxt = None
        if batch.state == BatchState.QUEUED.value:
            nxt = f"Query the batch again; {batch.completed} of {batch.size} are evaluated"
            if study.state == StudyState.PENDING_APPROVAL.value:
                nxt = "The study waits for a person's approval; the batch runs after it"
        return BatchView(batch_id=batch.id, study_id=study.id, sequence=batch.sequence,
                         first_evaluation=batch.offset + 1, state=batch.state, size=batch.size,
                         completed=batch.completed, results=results, next_action=nxt)

    # -- lifecycle -------------------------------------------------------------------

    def get_study(self, principal: Principal, study_id: str) -> StudyView:
        with self.tx() as s:
            return self._view(s, self.owned(s, Study, study_id, principal, "viewer"))

    def close_study(self, principal: Principal, study_id: str) -> StudyView:
        """No more batches; the study completes when the queued ones are evaluated."""
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "editor", lock=True)
            if study.state == StudyState.PENDING_APPROVAL.value:
                raise PlatformError(
                    ErrorCode.INVALID_TRANSITION,
                    f"study {study.id} waits for approval and cannot be closed yet",
                    next_action="Resolve the approval first, or cancel the study.")
            if study.state == StudyState.OPEN.value:
                study.state = StudyState.CLOSED.value
                study.updated_at = self.now()
                self.audit(s, study.organization_id, human_actor(principal), "study.close",
                           "study", study.id)
                self._maybe_complete(s, study)
            return self._view(s, study)

    def cancel_study(self, principal: Principal, study_id: str) -> StudyView:
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "editor", lock=True)
            if StudyState(study.state) not in STUDY_TERMINAL:
                self._end_study(s, study, StudyState.CANCELLED, {"reason": "cancelled by user"})
                self.audit(s, study.organization_id, human_actor(principal), "study.cancel",
                           "study", study.id)
        self._stop_pool(study_id)
        return self._view_by_id(study_id)

    def _view_by_id(self, study_id: str) -> StudyView:
        with self.tx() as s:
            return self._view(s, s.get(Study, study_id))

    def _stop_pool(self, study_id: str) -> None:
        """Ask the study's task jobs to stop, each in its own transaction."""
        with self.tx() as s:
            job_ids = list(s.scalars(select(Job.id).where(
                Job.study_id == study_id, Job.state.notin_(TERMINAL_JOB_STATES))))
        for job_id in job_ids:
            with self.tx() as s:
                job = s.get(Job, job_id, with_for_update=True)
                if JobState(job.state) in JOB_TERMINAL:
                    continue
                if JobState(job.state) in (JobState.ACCEPTED, JobState.LEASABLE):
                    self._set_state(job, JobState.CANCELLED, failure={"reason": "study ended"})
                else:
                    job.cancel_requested, job.updated_at = True, self.now()

    def _end_study(self, s: Session, study: Study, state: StudyState,
                   failure: dict | None) -> None:
        """End a study: cancel its queued chunks and batches. Task jobs learn on their next
        lease; callers that can do so without holding a job lock also stop them at once."""
        now = self.now()
        study.state, study.updated_at, study.terminal_at = state.value, now, now
        if failure is not None:
            study.failure = failure
        for chunk in s.scalars(select(ChunkRow).where(
                ChunkRow.study_id == study.id, ChunkRow.state.in_(["PENDING", "LEASED"]))
                .with_for_update()):
            chunk.state, chunk.holder_attempt_id, chunk.lease_expires_at = "CANCELLED", None, None
        batch_state = BatchState.FAILED if state == StudyState.FAILED else BatchState.CANCELLED
        for batch in s.scalars(select(StudyBatch).where(
                StudyBatch.study_id == study.id, StudyBatch.state == BatchState.QUEUED.value)):
            batch.state = batch_state.value
        # Tasks not yet running can be cancelled here without waiting on anyone.
        for job in s.scalars(select(Job).where(
                Job.study_id == study.id,
                Job.state.in_([JobState.ACCEPTED.value, JobState.LEASABLE.value]))
                .with_for_update(skip_locked=True)):
            self._set_state(job, JobState.CANCELLED, failure={"reason": f"study {state.value}"})

    def _maybe_complete(self, s: Session, study: Study) -> None:
        if study.state != StudyState.CLOSED.value:
            return
        queued = s.scalar(select(func.count()).select_from(StudyBatch).where(
            StudyBatch.study_id == study.id, StudyBatch.state == BatchState.QUEUED.value)) or 0
        if queued == 0:
            now = self.now()
            study.state, study.updated_at, study.terminal_at = (
                StudyState.COMPLETED.value, now, now)

    def resolve_study_approval(self, s: Session, approval: Approval, approve: bool,
                               note: str | None) -> str:
        study = s.get(Study, approval.study_id, with_for_update=True)
        # Only an approval opens a study that waits for one; nothing else leaves that state.
        if study.state == StudyState.PENDING_APPROVAL.value:
            if approve:
                study.state = StudyState.OPEN.value
                study.updated_at = self.now()
                self._top_up(s, study)
            else:
                self._end_study(s, study, StudyState.REJECTED,
                                {"reason": "approval rejected", "note": note})
        return study.state

    def expire_study_approval(self, s: Session, approval: Approval) -> None:
        study = s.get(Study, approval.study_id, with_for_update=True)
        if study.state == StudyState.PENDING_APPROVAL.value:
            self._end_study(s, study, StudyState.REJECTED,
                            {"reason": "approval expired unanswered"})

    # -- the pool ----------------------------------------------------------------------

    def _top_up(self, s: Session, study: Study) -> int:
        """Start task jobs until the pool has ``pool_size`` working tasks, or as many as
        there are chunks waiting."""
        if study.state not in (StudyState.OPEN.value, StudyState.CLOSED.value):
            return 0
        pending = s.scalar(select(func.count()).select_from(ChunkRow).where(
            ChunkRow.study_id == study.id, ChunkRow.state == "PENDING")) or 0
        active = s.scalar(select(func.count()).select_from(Job).where(
            Job.study_id == study.id, Job.state.notin_(TERMINAL_JOB_STATES),
            Job.retired_at.is_(None))) or 0
        wanted = min(study.pool_size, pending) - active
        template = JobSpec.model_validate(study.definition["task_spec"])
        for _ in range(max(wanted, 0)):
            job_id = new_id("job")
            spec = template.model_copy(update={"job_id": job_id})
            s.add(Job(id=job_id, run_id=study.run_id, organization_id=study.organization_id,
                      target_id=study.target_id, state=JobState.ACCEPTED.value,
                      spec=spec.model_dump(mode="json"), retry_budget=spec.limits.retry_budget,
                      study_id=study.id))
            s.flush()
            s.add(Outbox(topic="job.accepted", payload={"job_id": job_id}))
        return max(wanted, 0)

    def _release(self, s: Session, study: Study, chunk: ChunkRow, reason: str) -> None:
        """Return a chunk to the queue, or fail its candidates after too many deliveries."""
        chunk.holder_attempt_id, chunk.lease_expires_at = None, None
        if StudyState(study.state) in STUDY_TERMINAL:
            chunk.state = "CANCELLED"
            return
        if chunk.deliveries < MAX_DELIVERIES:
            chunk.state = "PENDING"
            return
        error = f"no result after {chunk.deliveries} deliveries ({reason})"
        self._record(s, study, chunk, [{"costs": None, "steps": 0, "error": error}]
                     * (chunk.stop - chunk.start), attempt_id=None, obj=None)

    def study_attempt_ended(self, s: Session, job: Job, attempt: Attempt,
                            failure_class: FailureClass | None) -> None:
        """Called with the job and attempt locked when an attempt of a task job ends."""
        if job.study_id is None:
            return
        study = s.get(Study, job.study_id, with_for_update=True)
        for chunk in list(s.scalars(select(ChunkRow).where(
                ChunkRow.holder_attempt_id == attempt.id, ChunkRow.state == "LEASED")
                .with_for_update())):
            if chunk.state == "LEASED":  # an earlier release may have ended the study
                self._release(s, study, chunk, f"attempt {attempt.number} of {job.id} ended")
        if StudyState(study.state) in STUDY_TERMINAL:
            return
        message = (attempt.diagnostics or {}).get("message")
        if JobState(job.state) == JobState.FAILED and failure_class in \
                (FailureClass.INVALID_INPUT, FailureClass.MISSING_DEPENDENCY):
            self._end_study(s, study, StudyState.FAILED, {
                "reason": "the evaluator could not run the study", "job_id": job.id,
                "class": failure_class.value, "message": message})
            return
        if JobState(job.state) in (JobState.FAILED, JobState.TIMED_OUT):
            # Tasks that keep failing without completing a chunk would be replaced forever.
            study.task_failures += 1
            if study.task_failures >= MAX_TASK_FAILURES:
                self._end_study(s, study, StudyState.FAILED, {
                    "reason": f"{study.task_failures} evaluator tasks failed without completing "
                              "a chunk", "job_id": job.id,
                    "class": failure_class.value if failure_class else None,
                    "message": message})
                return
        self._maybe_complete(s, study)
        self._top_up(s, study)

    def study_job_timed_out_in_queue(self, s: Session, job: Job) -> None:
        if job.study_id is None:
            return
        study = s.get(Study, job.study_id, with_for_update=True)
        others = s.scalar(select(func.count()).select_from(Job).where(
            Job.study_id == study.id, Job.id != job.id,
            Job.state.notin_(TERMINAL_JOB_STATES))) or 0
        if others == 0 and StudyState(study.state) not in STUDY_TERMINAL:
            self._end_study(s, study, StudyState.FAILED, {
                "reason": "no capacity claimed an evaluator task before its queue deadline",
                "job_id": job.id, "class": FailureClass.CAPACITY.value})

    # -- agent routes --------------------------------------------------------------------

    def evaluation_lease(self, target_id: str, attempt_token: str,
                         req: EvaluationLeaseRequest) -> EvaluationLeaseResponse:
        """Lease the next chunk to a task attempt, or tell it to finish."""
        self.tokens.check(attempt_token, target_id=target_id, attempt_id=req.attempt_id,
                          fence=req.fencing_token)
        now = self.now()
        with self.tx() as s:
            job, attempt = self._locked(s, target_id, req.attempt_id)  # type: ignore[attr-defined]
            self._authority(job, attempt, req.fencing_token)  # type: ignore[attr-defined]
            if job.study_id is None:
                raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                    f"job {job.id} is not an evaluator task")
            done = EvaluationLeaseResponse(done=True)
            if job.cancel_requested or job.retired_at is not None:
                return done
            study = s.get(Study, job.study_id, with_for_update=True)
            if study.state not in (StudyState.OPEN.value, StudyState.CLOSED.value):
                job.retired_at = now
                return done
            # A task evaluates one chunk at a time, so asking again while it holds one means the
            # answer was lost on the way (a timeout after commit): deliver the same lease again.
            held = s.scalar(
                select(ChunkRow).where(ChunkRow.holder_attempt_id == attempt.id,
                                       ChunkRow.state == "LEASED")
                .order_by(ChunkRow.batch_sequence, ChunkRow.start).limit(1).with_for_update())
            if held is not None:
                return EvaluationLeaseResponse(chunk=self._chunk(s, held))
            chunk = s.scalar(
                select(ChunkRow).where(ChunkRow.study_id == study.id, ChunkRow.state == "PENDING")
                .order_by(ChunkRow.batch_sequence, ChunkRow.start).limit(1)
                .with_for_update(skip_locked=True))
            if chunk is None:
                idle = float(study.definition.get("idle_timeout_seconds", 600))
                if (study.state == StudyState.CLOSED.value
                        or now - study.updated_at > timedelta(seconds=idle)):
                    # Retired under the study lock, so a batch submitted next starts a new task.
                    job.retired_at = now
                    return done
                return EvaluationLeaseResponse(poll_after_seconds=1.0)
            timeout = float(study.definition.get("evaluation_timeout_seconds", 600))
            chunk.state = "LEASED"
            chunk.lease_number += 1
            chunk.deliveries += 1
            chunk.holder_attempt_id = attempt.id
            chunk.lease_expires_at = now + timedelta(
                seconds=timeout * (chunk.stop - chunk.start) + self.settings.lease_seconds)
            return EvaluationLeaseResponse(chunk=self._chunk(s, chunk))

    @staticmethod
    def _chunk(s: Session, chunk: ChunkRow) -> EvaluationChunk:
        batch = s.get(StudyBatch, chunk.batch_id)
        return EvaluationChunk(
            chunk_id=chunk.id, lease_number=chunk.lease_number, study_id=chunk.study_id,
            batch_id=batch.id, start=chunk.start, first_evaluation=batch.offset + chunk.start + 1,
            candidates=batch.candidates[chunk.start:chunk.stop],
            lease_expires_at=chunk.lease_expires_at)

    def evaluation_complete(self, target_id: str, attempt_token: str,
                            body: EvaluationCompletion) -> CompletionAck:
        self.tokens.check(attempt_token, target_id=target_id, attempt_id=body.attempt_id,
                          fence=body.fencing_token)
        ended = None
        with self.tx() as s:
            job, attempt = self._locked(s, target_id, body.attempt_id)  # type: ignore[attr-defined]
            self._authority(job, attempt, body.fencing_token)  # type: ignore[attr-defined]
            study = s.get(Study, job.study_id, with_for_update=True) if job.study_id else None
            chunk = s.get(ChunkRow, body.chunk_id, with_for_update=True)
            if study is None or chunk is None or chunk.study_id != study.id:
                raise PlatformError(ErrorCode.NOT_FOUND, f"chunk {body.chunk_id} was not found")
            if (chunk.state == "DONE" and chunk.completed_attempt_id == attempt.id
                    and chunk.lease_number == body.lease_number):
                return CompletionAck(accepted=True)  # a retried report of the same result
            if (chunk.state != "LEASED" or chunk.lease_number != body.lease_number
                    or chunk.holder_attempt_id != attempt.id):
                return CompletionAck(accepted=False, reason=(
                    f"chunk {chunk.id} is at lease {chunk.lease_number} ({chunk.state}); "
                    f"the result for lease {body.lease_number} is discarded"))
            n_objectives = len(study.definition["objectives"])
            if len(body.results) != chunk.stop - chunk.start:
                raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                    f"chunk {chunk.id} has {chunk.stop - chunk.start} candidates; "
                                    f"{len(body.results)} results were reported")
            for r in body.results:
                if r.costs is not None and len(r.costs) != n_objectives:
                    raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                        f"each result needs {n_objectives} costs")
            spec = JobSpec.model_validate(job.spec)
            prefix = f"{spec.outputs.storage_prefix}{attempt.id}/evaluations/"
            if not body.object.key.startswith(prefix):
                raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                    f"evaluation records must be stored under {prefix}")
            self._record(s, study, chunk, [r.model_dump() for r in body.results],
                         attempt_id=attempt.id, obj=body.object.model_dump())
            if study.state == StudyState.FAILED.value:
                ended = study.id
            else:
                self._maybe_complete(s, study)
        if ended:
            self._stop_pool(ended)
        return CompletionAck(accepted=True)

    def _record(self, s: Session, study: Study, chunk: ChunkRow, results: list[dict],
                attempt_id: str | None, obj: dict | None) -> None:
        """Store a chunk's outcome, applying the study's failed-candidate policy."""
        definition = study.definition
        policy = FailurePolicy(definition["on_failure"])
        penalty = [float(definition["failure_penalty"])] * len(definition["objectives"])
        batch = s.get(StudyBatch, chunk.batch_id, with_for_update=True)
        records, failures = [], []
        for i, r in enumerate(results):
            evaluation = batch.offset + chunk.start + i + 1
            if r.get("error") is None:
                records.append({"evaluation": evaluation, "costs": r["costs"], "failed": False,
                                "error": None, "steps": r.get("steps", 0)})
                continue
            failures.append((evaluation, r["error"]))
            records.append({"evaluation": evaluation,
                            "costs": penalty if policy == FailurePolicy.PENALTY else None,
                            "failed": True, "error": r["error"], "steps": r.get("steps", 0)})
        now = self.now()
        chunk.state, chunk.results, chunk.completed_at = "DONE", records, now
        chunk.completed_attempt_id = attempt_id
        chunk.holder_attempt_id, chunk.lease_expires_at = None, None
        if obj is not None:
            chunk.object_key, chunk.object_sha256 = obj["key"], obj["sha256"]
            chunk.object_size = obj["size"]
        study.evaluations_completed += len(records)
        study.evaluations_failed += len(failures)
        study.updated_at = now
        if attempt_id is not None:
            study.task_failures = 0  # a task completed a chunk: the pool works
        batch.completed += len(records)
        s.flush()
        if batch.completed >= batch.size and batch.state == BatchState.QUEUED.value:
            rows = s.scalars(select(ChunkRow).where(ChunkRow.batch_id == batch.id)
                             .order_by(ChunkRow.start))
            batch.results = [rec for c in rows for rec in (c.results or [])]
            batch.state, batch.completed_at = BatchState.DONE.value, now
        if failures and policy == FailurePolicy.FAIL and \
                StudyState(study.state) not in STUDY_TERMINAL:
            evaluation, error = min(failures)
            self._end_study(s, study, StudyState.FAILED, {
                "reason": f"evaluation {evaluation} failed and the study's policy is 'fail'",
                "evaluation": evaluation, "error": error})

    # -- reconciler -------------------------------------------------------------------

    def reconcile_studies(self, limit: int = 200) -> dict[str, int]:
        """Re-offer chunks whose leases expired, and top up pools that have fewer working
        tasks than their size while work waits."""
        now = self.now()
        counts = {"chunk_leases_expired": 0, "pools_restarted": 0}
        with self.tx() as s:
            expired = list(s.execute(
                select(ChunkRow.id, ChunkRow.study_id).where(
                    ChunkRow.state == "LEASED", ChunkRow.lease_expires_at < now)
                .order_by(ChunkRow.lease_expires_at).limit(limit)))
            waiting = list(s.scalars(
                select(Study.id).where(
                    Study.state.in_([StudyState.OPEN.value, StudyState.CLOSED.value]),
                    select(ChunkRow.id).where(ChunkRow.study_id == Study.id,
                                              ChunkRow.state == "PENDING").exists())
                .order_by(Study.updated_at).limit(limit)))
        for chunk_id, study_id in expired:
            with self.tx() as s:
                study = s.get(Study, study_id, with_for_update=True)
                chunk = s.get(ChunkRow, chunk_id, with_for_update=True)
                if chunk.state != "LEASED" or chunk.lease_expires_at >= now:
                    continue
                self._release(s, study, chunk, "its lease expired")
                counts["chunk_leases_expired"] += 1
                if study.state == StudyState.FAILED.value:
                    continue
                self._maybe_complete(s, study)
                self._top_up(s, study)
        for study_id in waiting:  # pools short of tasks while work waits (e.g. a task cancelled)
            with self.tx() as s:
                study = s.get(Study, study_id, with_for_update=True)
                counts["pools_restarted"] += self._top_up(s, study) > 0
        return counts

    # -- results --------------------------------------------------------------------------

    def _done_chunks(self, s: Session, study: Study):
        return s.execute(
            select(ChunkRow, StudyBatch).join(StudyBatch, StudyBatch.id == ChunkRow.batch_id)
            .where(ChunkRow.study_id == study.id, ChunkRow.state == "DONE")
            .order_by(ChunkRow.batch_sequence, ChunkRow.start))

    def list_evaluations(self, principal: Principal, study_id: str, after: int = 0,
                         limit: int = 500) -> dict:
        """Evaluation records with their lineage, in submission order.

        Returns the evaluations numbered above ``after``, up to the first one not yet
        evaluated, so a reader that passes the last number it saw never misses one.
        """
        limit = max(1, min(limit, 2000))
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "viewer")
            rows = s.execute(
                select(ChunkRow, StudyBatch).join(StudyBatch, StudyBatch.id == ChunkRow.batch_id)
                .where(ChunkRow.study_id == study.id)
                .order_by(ChunkRow.batch_sequence, ChunkRow.start))
            page: list[dict] = []
            stopped = False  # at a chunk still running, or at the page limit
            for chunk, batch in rows:
                if batch.offset + chunk.stop <= after or chunk.state == "CANCELLED":
                    continue
                if chunk.state != "DONE" or len(page) >= limit:
                    stopped = True
                    break
                for i, rec in enumerate(chunk.results or []):
                    if rec["evaluation"] > after and len(page) < limit:
                        page.append({**rec, "decisions": batch.candidates[chunk.start + i],
                                     "batch_id": batch.id, "batch_sequence": batch.sequence,
                                     "chunk_id": chunk.id, "lease_number": chunk.lease_number,
                                     "attempt_id": chunk.completed_attempt_id,
                                     "object_key": chunk.object_key})
            last = page[-1]["evaluation"] if page else after
            # Complete: nothing later is listed or still to come, and the study takes no more.
            done = not stopped and StudyState(study.state) in STUDY_TERMINAL
            return {"evaluations": page, "next_after": last, "complete": done}

    def study_results(self, principal: Principal, study_id: str) -> dict:
        """The Pareto set (or the best candidate for one objective) over successful
        evaluations, by the same rules as ``openswmm_gymnasium.optimize.assemble_result``."""
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "viewer")
            names = [o["name"] for o in study.definition["objectives"]]
            ok: list[dict] = []
            count = 0
            for chunk, batch in self._done_chunks(s, study):
                for i, rec in enumerate(chunk.results or []):
                    count += 1
                    if not rec["failed"]:
                        ok.append({**rec, "decisions": batch.candidates[chunk.start + i]})
            ok.sort(key=lambda r: r["evaluation"])
            result: dict[str, Any] = {
                "study_id": study.id, "state": study.state, "objective_names": names,
                "evaluations_count": count, "failed_count": count - len(ok),
                "pareto": [], "best": None,
            }
            if ok:
                if len(names) == 1:
                    best = min(range(len(ok)), key=lambda i: (ok[i]["costs"][0], i))
                    front = [best]
                else:
                    front = pareto_indices([(i, r["costs"]) for i, r in enumerate(ok)])
                    best = min(range(len(ok)), key=lambda i: (sum(ok[i]["costs"]), i))
                result["pareto"] = [ok[i] for i in front]
                result["best"] = ok[best]
            return result

    def study_manifest(self, principal: Principal, study_id: str) -> dict:
        """Lineage of a study: its definition, every batch and chunk with the object holding
        its records, and every pool task attempt with its attempt manifest checksum."""
        with self.tx() as s:
            study = self.owned(s, Study, study_id, principal, "viewer")
            definition = {k: v for k, v in study.definition.items() if k != "task_spec"}
            task = study.definition["task_spec"]
            batches = [{"batch_id": b.id, "sequence": b.sequence, "offset": b.offset,
                        "size": b.size, "candidates_sha256": b.request_hash, "state": b.state}
                       for b in s.scalars(select(StudyBatch).where(StudyBatch.study_id == study.id)
                                          .order_by(StudyBatch.sequence))]
            chunks = [{"chunk_id": c.id, "batch_id": c.batch_id, "start": c.start,
                       "stop": c.stop, "state": c.state, "lease_number": c.lease_number,
                       "deliveries": c.deliveries, "attempt_id": c.completed_attempt_id,
                       "object": ({"key": c.object_key, "sha256": c.object_sha256,
                                   "size": c.object_size} if c.object_key else None)}
                      for c in s.scalars(select(ChunkRow).where(ChunkRow.study_id == study.id)
                                         .order_by(ChunkRow.batch_sequence, ChunkRow.start))]
            pool = []
            for job in s.scalars(select(Job).where(Job.study_id == study.id)
                                 .order_by(Job.created_at, Job.id)):
                attempts = s.scalars(select(Attempt).where(Attempt.job_id == job.id)
                                     .order_by(Attempt.number))
                pool.append({"job_id": job.id, "state": job.state, "attempts": [
                    {"attempt_id": a.id, "number": a.number, "state": a.state,
                     "failure_class": a.failure_class, "attempt_manifest_sha256": a.manifest_sha256}
                    for a in attempts]})
            body = {
                "study_id": study.id, "run_id": study.run_id, "state": study.state,
                "organization_id": study.organization_id, "project_id": study.project_id,
                "revision_id": study.revision_id,
                "revision_manifest_sha256": task["inputs"]["manifest_sha256"],
                "target_id": study.target_id, "actor": study.actor, "definition": definition,
                "workload": task["workload"], "runtime": task["runtime"],
                "max_evaluations": study.max_evaluations, "pool_size": study.pool_size,
                "counts": {"submitted": study.evaluations_submitted,
                           "completed": study.evaluations_completed,
                           "failed": study.evaluations_failed},
                "batches": batches, "chunks": chunks, "pool": pool,
            }
            return {**body, "manifest_sha256": sha256_of(body)}

    def _view(self, s: Session, study: Study) -> StudyView:
        pool = [{"job_id": j.id, "state": j.state, "retired": j.retired_at is not None,
                 "attempts_used": j.attempts_used}
                for j in s.scalars(select(Job).where(Job.study_id == study.id)
                                   .order_by(Job.created_at, Job.id))]
        approval = s.scalar(select(Approval.id).where(Approval.study_id == study.id,
                                                      Approval.state == "PENDING"))
        state = StudyState(study.state)
        nxt = {
            StudyState.PENDING_APPROVAL: f"A project editor must resolve approval {approval}",
            StudyState.OPEN: "Submit batches of candidates; close the study when done",
            StudyState.CLOSED: "The queued batches are being evaluated",
        }.get(state)
        return StudyView(
            study_id=study.id, run_id=study.run_id, project_id=study.project_id,
            session_id=study.session_id, revision_id=study.revision_id,
            target_id=study.target_id, state=state, workflow=study.definition["workflow"],
            workflow_version=study.definition["workflow_version"],
            objectives=[Objective.model_validate(o) for o in study.definition["objectives"]],
            dimensions=len(study.definition["dimensions"]), seed=study.definition["seed"],
            on_failure=study.definition["on_failure"], max_evaluations=study.max_evaluations,
            evaluations_submitted=study.evaluations_submitted,
            evaluations_completed=study.evaluations_completed,
            evaluations_failed=study.evaluations_failed, batches=study.batches_submitted,
            pool_size=study.pool_size, pool=pool, approval_id=approval, failure=study.failure,
            created_at=study.created_at, updated_at=study.updated_at, next_action=nxt)
