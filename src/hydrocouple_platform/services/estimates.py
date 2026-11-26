"""Estimate before spend: what a run or study would cost, kept so a submission can name it.

An estimate prices a would-be submission from its target's price table (the price of an
hour of each resource profile) and the hours the caller states; without either, its spend
is unknown and its cost band says so. The evaluation and concurrency budgets stay the
real-time controls: prices come from a table, not from a bill. An organization can
require a matching, unexpired estimate for any submission above a number of evaluations.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.requests import AgentSessionView, Budget, OrganizationPolicy, PriceTable
from ..contracts.states import JOB_HELD, JOB_TERMINAL, JobState
from ..contracts.studies import StudyState
from ..db.models import (
    AgentSession,
    ComputeTarget,
    EstimateRow,
    Job,
    Organization,
    Project,
    Run,
    Study,
    new_id,
)
from .auth import Principal
from .base import ServiceBase

ACTIVE_STATES = [s.value for s in JobState if s not in JOB_TERMINAL]
# Upper edges of the cost bands, in the price table's currency.
BAND_EDGES = (1, 10, 100, 1000)


def cost_band(spend: float | None, currency: str | None) -> str:
    """A band rather than a figure: the price table is an estimate, not a bill."""
    if spend is None:
        return "unknown"
    for edge in BAND_EDGES:
        if spend < edge:
            return f"under {edge} {currency}"
    return f"{BAND_EDGES[-1]} {currency} or more"


class EstimateServices(ServiceBase):
    # -- pricing and the session's use ---------------------------------------------------

    @staticmethod
    def _spend(target: ComputeTarget, profile: str | None,
               hours: float | None) -> tuple[float | None, str | None]:
        """Estimated spend and its currency; spend is None without a price or the hours."""
        if target.price_table is None:
            return None, None
        table = PriceTable.model_validate(target.price_table)
        price = table.per_hour.get(profile) if profile else None
        if price is None or hours is None:
            return None, table.currency
        return round(price * hours, 6), table.currency

    @staticmethod
    def _session_use(s: Session, session: AgentSession) -> dict[str, float]:
        """What an agent session has used of each budget limit."""
        active = s.scalar(
            select(func.count()).select_from(Job).join(Run, Run.id == Job.run_id)
            .where(Run.agent_session_id == session.id, Job.state.in_(ACTIVE_STATES),
                   Job.study_id.is_(None))) or 0
        # Every live study's whole pool, which can start at any time.
        pools = s.scalar(
            select(func.coalesce(func.sum(Study.pool_size), 0))
            .where(Study.agent_session_id == session.id,
                   Study.state.in_(["PENDING_APPROVAL", "OPEN", "CLOSED"]))) or 0
        reserved = s.scalar(
            select(func.coalesce(func.sum(Study.max_evaluations), 0))
            .where(Study.agent_session_id == session.id, Study.state != "REJECTED")) or 0
        return {"runs": session.runs_submitted, "evaluations": reserved,
                "concurrent_attempts": active + pools,
                "estimated_spend": session.estimated_spend}

    @staticmethod
    def _work_ends_at(session: AgentSession) -> datetime | None:
        seconds = Budget.model_validate(session.budget).max_wall_time_seconds
        return None if seconds is None else session.created_at + timedelta(seconds=seconds)

    def _check_wall_time(self, session: AgentSession) -> None:
        """Refuse new work from a session whose wall time is over."""
        ends = self._work_ends_at(session)
        if ends is not None and ends <= self.now():
            raise PlatformError(
                ErrorCode.FORBIDDEN,
                f"the agent session's wall time ended at {ends.isoformat()}; it takes no new work",
                next_action="Its results stay readable; ask the person for a new agent session "
                            "to run more.")

    @staticmethod
    def _spend_reason(budget: Budget, used: dict[str, float], spend: float | None,
                      currency: str | None, what: str) -> str | None:
        if budget.max_estimated_spend is None:
            return None
        if spend is None:
            return (f"the session caps spend, but the cost of this {what} is unknown: its target "
                    "has no price for its profile, or no runtime was stated")
        if used["estimated_spend"] + spend > budget.max_estimated_spend:
            return (f"this {what} is estimated at {spend:g} {currency}; the session has "
                    f"{max(budget.max_estimated_spend - used['estimated_spend'], 0):g} of "
                    f"{budget.max_estimated_spend:g} left")
        return None

    # -- estimates a submission names ------------------------------------------------------

    def _record_estimate(self, s: Session, principal: Principal, project: Project, kind: str,
                         request_hash: str, evaluations: int, spend: float | None,
                         requires_approval: bool) -> EstimateRow:
        row = EstimateRow(
            id=new_id("est"), organization_id=project.organization_id, project_id=project.id,
            user_id=principal.user_id, agent_session_id=principal.agent_session_id, kind=kind,
            request_hash=request_hash, evaluations=evaluations, estimated_spend=spend,
            requires_approval=requires_approval,
            expires_at=self.now() + timedelta(seconds=self.settings.estimate_ttl_seconds))
        s.add(row)
        s.flush()
        return row

    def _require_estimate(self, s: Session, principal: Principal, organization_id: str,
                          kind: str, request_hash: str, evaluations: int,
                          estimate_id: str | None) -> None:
        """Refuse a submission the organization's policy says needs an estimate it lacks."""
        org = s.get(Organization, organization_id)
        threshold = OrganizationPolicy.model_validate(
            org.policy or {}).require_estimate_above_evaluations
        if threshold is None or evaluations <= threshold:
            return
        row = s.get(EstimateRow, estimate_id) if estimate_id else None
        if estimate_id is None:
            problem = "and the request names none"
        elif row is None or row.organization_id != organization_id \
                or row.user_id != principal.user_id \
                or row.agent_session_id != principal.agent_session_id:
            problem = f"and {estimate_id} is not an estimate this caller made"
        elif row.kind != kind or row.request_hash != request_hash:
            problem = f"and {estimate_id} estimated a different request"
        elif row.expires_at <= self.now():
            problem = f"and {estimate_id} expired"
        else:
            return
        raise PlatformError(
            ErrorCode.ESTIMATE_REQUIRED,
            f"the organization requires an estimate for a {kind} of more than {threshold} "
            f"evaluations; this one has {evaluations}, {problem}",
            details={"threshold": threshold, "evaluations": evaluations},
            next_action=f"Estimate exactly this {kind}, then submit it with that estimate_id.")

    # -- policy, prices and the session view -----------------------------------------------

    def set_policy(self, principal: Principal, organization_id: str,
                   policy: OrganizationPolicy) -> dict[str, Any]:
        self.human_only(principal, "set organization policy")
        self.require(principal, organization_id, "admin", f"organization {organization_id}")
        with self.tx() as s:
            org = s.get(Organization, organization_id, with_for_update=True)
            org.policy = policy.model_dump(mode="json")
            self.audit(s, organization_id, self.audit_actor(s, principal), "organization.policy",
                       "organization", organization_id, {"policy": org.policy})
            return org.policy

    def set_target_prices(self, principal: Principal, target_id: str,
                          table: PriceTable) -> dict[str, Any]:
        """Replace a target's price table (a price source refreshed as prices change)."""
        self.human_only(principal, "set target prices")
        with self.tx() as s:
            target = self.owned(s, ComputeTarget, target_id, principal, "admin", lock=True)
            target.price_table = table.model_dump(mode="json")
            self.audit(s, target.organization_id, self.audit_actor(s, principal),
                       "target.prices", "target", target.id, {"price_table": target.price_table})
            return target.price_table

    def agent_session_view(self, principal: Principal) -> AgentSessionView:
        """An agent session's budget, what it has used, and what is left."""
        if principal.agent_session_id is None:
            raise PlatformError(ErrorCode.NOT_FOUND, "this credential is not an agent session's",
                                next_action="Call it with the agent session's own credential.")
        with self.tx() as s:
            row = s.get(AgentSession, principal.agent_session_id)
            budget = Budget.model_validate(row.budget)
            used = self._session_use(s, row)
            used["wall_time_seconds"] = round(
                max((self.now() - row.created_at).total_seconds(), 0.0), 3)
            limits = {"runs": budget.max_runs, "evaluations": budget.max_evaluations,
                      "concurrent_attempts": budget.max_concurrent_attempts,
                      "estimated_spend": budget.max_estimated_spend,
                      "wall_time_seconds": budget.max_wall_time_seconds}
            remaining = {k: None if limit is None else max(limit - used[k], 0)
                         for k, limit in limits.items()}
            return AgentSessionView(agent_session_id=row.id, expires_at=row.expires_at,
                                    budget=budget, used=used, remaining=remaining)

    # -- the wall time ---------------------------------------------------------------------

    def reconcile_agent_sessions(self, limit: int = 200) -> dict[str, int]:
        """End the work of sessions past their wall time: cancel their active runs and live
        studies, once, each session in its own transaction."""
        counts = {"sessions_out_of_time": 0}
        now = self.now()
        with self.tx() as s:
            candidates = list(s.scalars(
                select(AgentSession).where(AgentSession.work_ended_at.is_(None),
                                           AgentSession.revoked_at.is_(None))
                .order_by(AgentSession.created_at).limit(limit)))
            due = [row.id for row in candidates
                   if (ends := self._work_ends_at(row)) is not None and ends <= now]
        for session_id in due:
            studies: list[str] = []
            with self.tx() as s:
                row = s.get(AgentSession, session_id, with_for_update=True)
                if row.work_ended_at is not None:
                    continue
                row.work_ended_at = now
                jobs = 0
                for job in s.scalars(
                        select(Job).join(Run, Run.id == Job.run_id)
                        .where(Run.agent_session_id == row.id, Job.study_id.is_(None),
                               Job.state.in_(ACTIVE_STATES)).with_for_update()):
                    if JobState(job.state) in JOB_HELD:
                        job.cancel_requested, job.updated_at = True, now
                    else:
                        self._set_state(job, JobState.CANCELLED, failure={
                            "reason": "the agent session's wall time ended"})
                    jobs += 1
                studies = [st.id for st in s.scalars(select(Study).where(
                    Study.agent_session_id == row.id,
                    Study.state.in_(["PENDING_APPROVAL", "OPEN", "CLOSED"])).with_for_update())]
                for study_id in studies:
                    self._end_study(  # type: ignore[attr-defined]
                        s, s.get(Study, study_id), StudyState.CANCELLED,
                        {"reason": "the agent session's wall time ended"})
                self.audit(s, row.organization_id, {"system": "reconciler"},
                           "agent_session.wall_time", "agent_session", row.id,
                           {"jobs_cancelled": jobs, "studies_cancelled": studies})
            for study_id in studies:
                self._stop_pool(study_id)  # type: ignore[attr-defined]
            counts["sessions_out_of_time"] += 1
        return counts
