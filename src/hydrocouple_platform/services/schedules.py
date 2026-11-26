"""Schedules: the control plane submits a digital twin's cycles one after another.

Each cycle is an ordinary run, submitted as the schedule's creator (and charged to the agent
session that created the schedule, if one did), continuing from the previous cycle's saved
state. The reconciler records each cycle's outcome and submits the next one; a cycle that does
not succeed pauses the schedule.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.manifests import sha256_of
from ..contracts.requests import RunRequest
from ..contracts.schedules import (
    Pace,
    ProposalCreate,
    ProposalDecision,
    ProposalState,
    ProposalView,
    ScheduleCreate,
    ScheduleCycleView,
    ScheduleState,
    ScheduleView,
)
from ..contracts.states import JOB_TERMINAL, JobState
from ..db.models import (
    Attempt,
    Job,
    Run,
    Schedule,
    ScheduleCycle,
    SetpointProposal,
    User,
    new_id,
)
from ..workflows import get_workflow
from .auth import Principal
from .base import ServiceBase

TERMINAL_JOB_STATES = {s.value for s in JOB_TERMINAL}


def _cycle_end(schedule: Schedule, number: int) -> datetime:
    return schedule.first_cycle_end + timedelta(minutes=schedule.cycle_minutes * number)


class ScheduleServices(ServiceBase):
    # -- requests ------------------------------------------------------------------------

    @staticmethod
    def _cycle_request(req: ScheduleCreate | Schedule, number: int, cycle_end: datetime,
                       resume_from: str | None,
                       setpoints: list[dict] | None = None) -> RunRequest:
        parameters = {**req.parameters, "cycle_end": cycle_end.isoformat(timespec="seconds")}
        if setpoints:
            parameters["setpoints"] = setpoints
        return RunRequest(
            project_id=req.project_id, session_id=req.session_id, revision_id=req.revision_id,
            workflow=req.workflow, workflow_version=req.workflow_version, parameters=parameters,
            target_id=req.target_id, profile=req.profile, retry_budget=req.retry_budget,
            estimated_runtime_seconds=req.estimated_runtime_seconds, resume_from=resume_from)

    def _check_schedule(self, s: Session, principal: Principal, req: ScheduleCreate):
        workflow = get_workflow(req.workflow, req.workflow_version)
        if not workflow.resumable:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                f"{workflow.name}@{workflow.version} does not continue from an "
                                "earlier run's state, so it cannot run on a schedule")
        for key in ("cycle_end", "setpoints"):
            if key in req.parameters:
                raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                    f"the schedule sets each cycle's {key}; leave it out")
        if req.first_cycle_end.tzinfo is not None or req.first_cycle_end.microsecond:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "first_cycle_end is the model's wall clock in whole seconds; give "
                                "it without a zone")
        if (req.cycle_minutes * 60) % 1:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "cycle_minutes must be a whole number of seconds")
        if req.pace == Pace.REPLAY and req.lag_seconds:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "lag_seconds applies to wall_clock schedules only")
        # The first cycle, checked as its run would be when it is submitted.
        checked = self._check(  # type: ignore[attr-defined]
            s, principal, self._cycle_request(req, 0, req.first_cycle_end, req.resume_from),
            "editor")
        request_hash = sha256_of({**req.model_dump(mode="json", exclude={"tool_call_id"}),
                                  "agent_session_id": principal.agent_session_id})
        return checked, request_hash

    # -- create, view, pause, resume ---------------------------------------------------

    def create_schedule(self, principal: Principal, req: ScheduleCreate,
                        idempotency_key: str) -> ScheduleView:
        if not 8 <= len(idempotency_key) <= 200:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "the idempotency key must be 8 to 200 characters")
        try:
            return self._create_schedule(principal, req, idempotency_key)
        except IntegrityError:
            with self.tx() as s:  # a concurrent request with the same key won the insert
                checked, request_hash = self._check_schedule(s, principal, req)
                existing = self._existing_schedule(s, checked.project.organization_id,
                                                   idempotency_key, request_hash)
                if existing is None:
                    raise
                return self._schedule_view(s, existing)

    @staticmethod
    def _existing_schedule(s: Session, org_id: str, key: str,
                           request_hash: str) -> Schedule | None:
        schedule = s.scalar(select(Schedule).where(Schedule.organization_id == org_id,
                                                   Schedule.idempotency_key == key))
        if schedule is not None and schedule.request_hash != request_hash:
            raise PlatformError(ErrorCode.CONFLICT,
                                "this idempotency key was already used for a different schedule",
                                next_action="Use a new idempotency key for a different schedule.")
        return schedule

    def _create_schedule(self, principal: Principal, req: ScheduleCreate,
                         key: str) -> ScheduleView:
        with self.tx() as s:
            checked, request_hash = self._check_schedule(s, principal, req)
            org_id = checked.project.organization_id
            existing = self._existing_schedule(s, org_id, key, request_hash)
            if existing is not None:
                return self._schedule_view(s, existing)
            if checked.findings:
                raise PlatformError(
                    ErrorCode.CAPABILITY_UNSUPPORTED if checked.target.capabilities
                    else ErrorCode.NOT_READY,
                    "the schedule cannot be accepted: " + "; ".join(checked.findings),
                    details={"findings": checked.findings},
                    next_action="Fix the findings, then create the schedule again.")
            actor = {"human_subject": principal.subject, "tool_call_id": req.tool_call_id}
            if principal.agent_session_id:
                session = self._agent_session(  # type: ignore[attr-defined]
                    s, principal, principal.agent_session_id, checked.project, lock=False)
                actor["agent_session_id"] = session.id
            schedule = Schedule(
                id=new_id("sch"), organization_id=org_id, project_id=checked.project.id,
                session_id=req.session_id, revision_id=checked.revision.id,
                target_id=checked.target.id, workflow=checked.workflow.name,
                workflow_version=checked.workflow.version, parameters=req.parameters,
                first_cycle_end=req.first_cycle_end, cycle_minutes=req.cycle_minutes,
                cycles=req.cycles, pace=req.pace.value, lag_seconds=req.lag_seconds,
                resume_from=req.resume_from, await_decisions=req.await_decisions,
                profile=req.profile, retry_budget=req.retry_budget,
                estimated_runtime_seconds=req.estimated_runtime_seconds,
                state=ScheduleState.ACTIVE.value, next_cycle=0, tries=0,
                idempotency_key=key, request_hash=request_hash, actor=actor,
                agent_session_id=principal.agent_session_id, created_by=principal.user_id)
            s.add(schedule)
            s.flush()
            self.audit(s, org_id, actor, "schedule.create", "schedule", schedule.id,
                       {"workflow": f"{schedule.workflow}@{schedule.workflow_version}",
                        "cycles": schedule.cycles, "pace": schedule.pace})
            return self._schedule_view(s, schedule)

    def get_schedule(self, principal: Principal, schedule_id: str) -> ScheduleView:
        with self.tx() as s:
            return self._schedule_view(
                s, self.owned(s, Schedule, schedule_id, principal, "viewer"))

    def pause_schedule(self, principal: Principal, schedule_id: str) -> ScheduleView:
        """Submit no more cycles; a cycle already running finishes and is recorded."""
        with self.tx() as s:
            schedule = self.owned(s, Schedule, schedule_id, principal, "editor", lock=True)
            if schedule.state == ScheduleState.ACTIVE.value:
                schedule.state = ScheduleState.PAUSED.value
                schedule.pause_reason = {"reason": "paused by a person",
                                         "by": principal.subject}
                schedule.updated_at = self.now()
                self.audit(s, schedule.organization_id, {"human_subject": principal.subject},
                           "schedule.pause", "schedule", schedule.id, None)
            return self._schedule_view(s, schedule)

    def resume_schedule(self, principal: Principal, schedule_id: str) -> ScheduleView:
        """Continue a paused schedule; a cycle that did not succeed is submitted again."""
        with self.tx() as s:
            schedule = self.owned(s, Schedule, schedule_id, principal, "editor", lock=True)
            if schedule.state == ScheduleState.COMPLETED.value:
                raise PlatformError(ErrorCode.CONFLICT, f"schedule {schedule.id} has completed")
            if schedule.state == ScheduleState.PAUSED.value:
                schedule.state = ScheduleState.ACTIVE.value
                schedule.pause_reason = None
                schedule.updated_at = self.now()
                self.audit(s, schedule.organization_id, {"human_subject": principal.subject},
                           "schedule.resume", "schedule", schedule.id,
                           {"next_cycle": schedule.next_cycle})
            return self._schedule_view(s, schedule)

    def _schedule_view(self, s: Session, schedule: Schedule) -> ScheduleView:
        rows = s.execute(
            select(ScheduleCycle, Job.state).join(Job, Job.id == ScheduleCycle.job_id)
            .where(ScheduleCycle.schedule_id == schedule.id)
            .order_by(ScheduleCycle.number, ScheduleCycle.attempt)).all()
        return ScheduleView(
            schedule_id=schedule.id, state=ScheduleState(schedule.state),
            project_id=schedule.project_id, revision_id=schedule.revision_id,
            target_id=schedule.target_id, workflow=schedule.workflow,
            workflow_version=schedule.workflow_version, parameters=schedule.parameters,
            first_cycle_end=schedule.first_cycle_end, cycle_minutes=schedule.cycle_minutes,
            cycles=schedule.cycles, pace=Pace(schedule.pace), lag_seconds=schedule.lag_seconds,
            await_decisions=schedule.await_decisions, next_cycle=schedule.next_cycle,
            last_run_id=schedule.last_run_id,
            pause_reason=schedule.pause_reason, agent_session_id=schedule.agent_session_id,
            history=[ScheduleCycleView(number=c.number, cycle_end=c.cycle_end, attempt=c.attempt,
                                       run_id=c.run_id, job_id=c.job_id, job_state=state,
                                       resumed_from=c.resumed_from) for c, state in rows],
            created_at=schedule.created_at, updated_at=schedule.updated_at)

    # -- reconciler ----------------------------------------------------------------------

    def reconcile_schedules(self, limit: int = 200) -> dict[str, int]:
        """Record finished cycles and submit due ones, each schedule in its own transactions."""
        counts = {"cycles_submitted": 0, "cycles_succeeded": 0, "schedules_paused": 0,
                  "schedules_completed": 0}
        with self.tx() as s:
            ids = list(s.scalars(
                select(Schedule.id).where(or_(Schedule.state == ScheduleState.ACTIVE.value,
                                              Schedule.current_run_id.is_not(None)))
                .order_by(Schedule.updated_at).limit(limit)))
        for schedule_id in ids:
            self._tick_schedule(schedule_id, counts)
        return counts

    def _pause(self, s: Session, schedule: Schedule, reason: dict, counts: dict) -> None:
        schedule.state = ScheduleState.PAUSED.value
        schedule.pause_reason = reason
        schedule.updated_at = self.now()
        counts["schedules_paused"] += 1
        self.audit(s, schedule.organization_id, {"system": "scheduler"}, "schedule.pause",
                   "schedule", schedule.id, reason)

    def _tick_schedule(self, schedule_id: str, counts: dict[str, int]) -> None:
        with self.tx() as s:
            schedule = s.get(Schedule, schedule_id, with_for_update=True)
            if schedule.current_run_id is not None:
                job = s.scalar(select(Job).where(Job.run_id == schedule.current_run_id))
                if job.state not in TERMINAL_JOB_STATES:
                    return
                run = s.get(Run, schedule.current_run_id)
                if job.state == JobState.SUCCEEDED.value and run.canonical_attempt_id:
                    self._record_setpoints(s, schedule, run,
                                           _cycle_end(schedule, schedule.next_cycle))
                    schedule.last_run_id = run.id
                    schedule.next_cycle += 1
                    schedule.tries = 0
                    counts["cycles_succeeded"] += 1
                    if schedule.cycles is not None and schedule.next_cycle >= schedule.cycles:
                        schedule.state = ScheduleState.COMPLETED.value
                        counts["schedules_completed"] += 1
                else:
                    self._pause(s, schedule, {
                        "reason": "a cycle did not succeed", "cycle": schedule.next_cycle,
                        "run_id": run.id, "job_id": job.id, "job_state": job.state,
                        "failure": job.failure}, counts)
                schedule.current_run_id = None
                schedule.updated_at = self.now()
        with self.tx() as s:
            schedule = s.get(Schedule, schedule_id)
            if schedule.state != ScheduleState.ACTIVE.value or schedule.current_run_id:
                return
            number, tries = schedule.next_cycle, schedule.tries
            cycle_end = _cycle_end(schedule, number)
            due = cycle_end.replace(tzinfo=timezone.utc) + timedelta(
                seconds=schedule.lag_seconds)
            if schedule.pace == Pace.WALL_CLOCK.value and self.now() < due:
                return
            if schedule.await_decisions and s.scalar(select(SetpointProposal.id).where(
                    SetpointProposal.schedule_id == schedule.id,
                    SetpointProposal.state == ProposalState.PROPOSED.value).limit(1)):
                return  # a person has not decided every proposal yet
            resume_from = schedule.resume_from if number == 0 else schedule.last_run_id
            approved = s.scalars(select(SetpointProposal).where(
                SetpointProposal.schedule_id == schedule.id,
                SetpointProposal.state == ProposalState.APPROVED.value,
                SetpointProposal.effective_from < cycle_end)
                .order_by(SetpointProposal.effective_from, SetpointProposal.created_at))
            setpoints = [{"link": p.link, "value": p.value,
                          "effective_from": p.effective_from.isoformat(timespec="seconds"),
                          "proposal_id": p.id} for p in approved]
            req = self._cycle_request(schedule, number, cycle_end, resume_from, setpoints)
            principal = self._principal(s, s.get(User, schedule.created_by),
                                        schedule.agent_session_id)
        key = f"schedule/{schedule_id}/cycle/{number}/try/{tries}"
        accepted, error = None, None
        try:  # its own transaction, idempotent by key: a repeat returns the same run
            accepted = self.submit_run(principal, req, key)  # type: ignore[attr-defined]
        except PlatformError as exc:
            error = exc
        with self.tx() as s:
            schedule = s.get(Schedule, schedule_id, with_for_update=True)
            if (schedule.current_run_id is not None or schedule.next_cycle != number
                    or schedule.tries != tries or schedule.state == ScheduleState.COMPLETED.value):
                return  # another reconciler recorded this submission first
            if error is not None:
                if schedule.state == ScheduleState.ACTIVE.value:
                    self._pause(s, schedule, {
                        "reason": "a cycle could not be submitted", "cycle": number,
                        "error": error.code.value, "message": error.body.message}, counts)
                return
            schedule.current_run_id = accepted.run_id
            schedule.tries = tries + 1
            schedule.updated_at = self.now()
            s.add(ScheduleCycle(schedule_id=schedule.id, number=number, attempt=tries + 1,
                                cycle_end=cycle_end, run_id=accepted.run_id,
                                job_id=accepted.job_id, resumed_from=resume_from))
            counts["cycles_submitted"] += 1

    # -- setpoint proposals ----------------------------------------------------------------

    def _record_setpoints(self, s: Session, schedule: Schedule, run: Run,
                          cycle_end: datetime) -> None:
        """A succeeded cycle's applied setpoints and its forecast's proposals; proposals of
        earlier forecasts nobody decided are superseded by the new ones."""
        quality = (s.get(Attempt, run.canonical_attempt_id).manifest or {}).get("quality") or {}
        for applied in quality.get("setpoints_applied") or []:
            proposal = s.get(SetpointProposal, applied["proposal_id"], with_for_update=True)
            if proposal is not None and proposal.schedule_id == schedule.id:
                proposal.state = ProposalState.APPLIED.value
                proposal.applied_run_id = run.id
                proposal.applied_at = datetime.fromisoformat(applied["applied_at"])
        proposals = quality.get("proposals") or []
        if not proposals:
            return
        s.execute(update(SetpointProposal).where(
            SetpointProposal.schedule_id == schedule.id,
            SetpointProposal.state == ProposalState.PROPOSED.value,
            SetpointProposal.run_id.is_not(None)).values(state=ProposalState.SUPERSEDED.value))
        for p in proposals:
            s.add(SetpointProposal(
                id=new_id("spp"), organization_id=schedule.organization_id,
                project_id=schedule.project_id, schedule_id=schedule.id,
                state=ProposalState.PROPOSED.value, link=p["link"], property=p["property"],
                value=float(p["value"]), effective_from=datetime.fromisoformat(
                    p["effective_from"]), issued_at=cycle_end, run_id=run.id,
                proposed_by={"kind": "forecast", "run_id": run.id}))

    def propose_setpoint(self, principal: Principal, schedule_id: str,
                         req: ProposalCreate) -> ProposalView:
        """An agent's or a person's proposal; it waits for a person's decision like any other."""
        if req.effective_from.tzinfo is not None or req.effective_from.microsecond:
            raise PlatformError(ErrorCode.VALIDATION_ERROR,
                                "effective_from is the model's wall clock in whole seconds; give "
                                "it without a zone")
        with self.tx() as s:
            schedule = self.owned(s, Schedule, schedule_id, principal, "editor")
            by = {"kind": "agent" if principal.agent_session_id else "person",
                  "human_subject": principal.subject,
                  "agent_session_id": principal.agent_session_id,
                  "tool_call_id": req.tool_call_id}
            proposal = SetpointProposal(
                id=new_id("spp"), organization_id=schedule.organization_id,
                project_id=schedule.project_id, schedule_id=schedule.id,
                state=ProposalState.PROPOSED.value, link=req.link,
                property="link.target_setting", value=req.value,
                effective_from=req.effective_from, proposed_by=by, note=req.note)
            s.add(proposal)
            s.flush()
            self.audit(s, schedule.organization_id, by, "proposal.create", "proposal",
                       proposal.id, {"schedule_id": schedule.id, "link": req.link,
                                     "value": req.value})
            return self._proposal_view(s, proposal)

    def decide_proposal(self, principal: Principal, proposal_id: str,
                        req: ProposalDecision) -> ProposalView:
        self.human_only(principal, "approve or reject a setpoint")
        with self.tx() as s:
            proposal = self.owned(s, SetpointProposal, proposal_id, principal, "editor",
                                  lock=True)
            if proposal.state != ProposalState.PROPOSED.value:
                raise PlatformError(ErrorCode.CONFLICT,
                                    f"proposal {proposal.id} is {proposal.state}, not PROPOSED")
            proposal.state = (ProposalState.APPROVED if req.approve
                              else ProposalState.REJECTED).value
            proposal.decided_by = principal.user_id
            proposal.decided_at = self.now()
            proposal.decision_note = req.note
            s.flush()
            self.audit(s, proposal.organization_id, {"human_subject": principal.subject},
                       "proposal.decide", "proposal", proposal.id, {"state": proposal.state})
            return self._proposal_view(s, proposal)

    def list_proposals(self, principal: Principal, schedule_id: str) -> list[ProposalView]:
        with self.tx() as s:
            schedule = self.owned(s, Schedule, schedule_id, principal, "viewer")
            return [self._proposal_view(s, p) for p in s.scalars(
                select(SetpointProposal).where(SetpointProposal.schedule_id == schedule.id)
                .order_by(SetpointProposal.created_at, SetpointProposal.effective_from))]

    @staticmethod
    def _proposal_view(s: Session, p: SetpointProposal) -> ProposalView:
        decided_by = s.get(User, p.decided_by).subject if p.decided_by else None
        return ProposalView(
            proposal_id=p.id, schedule_id=p.schedule_id, state=ProposalState(p.state),
            link=p.link, property=p.property, value=p.value, effective_from=p.effective_from,
            issued_at=p.issued_at, run_id=p.run_id, proposed_by=p.proposed_by, note=p.note,
            decided_by=decided_by, decided_at=p.decided_at, decision_note=p.decision_note,
            applied_run_id=p.applied_run_id, applied_at=p.applied_at, created_at=p.created_at)

    def check_setpoints(self, s: Session, project_id: str, setpoints: list[dict]) -> None:
        """A run applies only setpoints a person approved, exactly as approved."""
        for sp in setpoints:
            proposal = s.get(SetpointProposal, sp.get("proposal_id"))
            if (proposal is None or proposal.project_id != project_id
                    or proposal.state != ProposalState.APPROVED.value
                    or proposal.link != sp["link"] or proposal.value != float(sp["value"])
                    or proposal.effective_from != datetime.fromisoformat(sp["effective_from"])):
                raise PlatformError(
                    ErrorCode.FORBIDDEN,
                    f"setpoint {sp.get('proposal_id')} is not a proposal a person approved",
                    next_action="Propose the setpoint and ask a person to approve it.")
