"""Shared plumbing for the application services: transactions, clock, authorization."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, TypeVar

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from ..config import ControlPlaneSettings
from ..contracts.agent import GrantMode
from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.states import JOB_TERMINAL, JobState, job_can
from ..db import transaction
from ..db.models import AuditRecord, GrantRequestRow, Job, Membership, User, new_id, utcnow
from .auth import ROLE_RANK, AttemptTokens, Principal, VerifiedClaims

T = TypeVar("T")


class ServiceBase:
    def __init__(
        self,
        factory: sessionmaker[Session],
        settings: ControlPlaneSettings,
        clock: Callable[[], datetime] = utcnow,
    ):
        self._factory = factory
        self.settings = settings
        self._clock = clock
        self.tokens = AttemptTokens(
            settings.attempt_token_secret.get_secret_value(), settings.attempt_token_seconds
        )

    def now(self) -> datetime:
        return self._clock()

    def tx(self):
        return transaction(self._factory)

    # -- principals ------------------------------------------------------------

    def principal_for(self, claims: VerifiedClaims) -> Principal:
        """Map verified sign-in claims to a principal; roles come from our records."""
        with self.tx() as s:
            # Insert-if-absent so two first requests from a new user cannot collide.
            s.execute(
                pg_insert(User)
                .values(id=new_id("usr"), issuer=claims.issuer, subject=claims.subject,
                        email=claims.email, created_at=self.now())
                .on_conflict_do_nothing(index_elements=["issuer", "subject"])
            )
            user = s.scalar(
                select(User).where(User.issuer == claims.issuer, User.subject == claims.subject)
            )
            return self._principal(s, user)

    @staticmethod
    def _principal(s: Session, user: User, agent_session_id: str | None = None) -> Principal:
        roles = {
            m.organization_id: m.role
            for m in s.scalars(select(Membership).where(Membership.user_id == user.id))
        }
        return Principal(
            user_id=user.id, issuer=user.issuer, subject=user.subject, email=user.email,
            roles=roles, agent_session_id=agent_session_id,
        )

    @staticmethod
    def human_only(principal: Principal, action: str) -> None:
        """Approvals, administration and new agent sessions need a person, not an agent."""
        if principal.agent_session_id is not None:
            raise PlatformError(
                ErrorCode.FORBIDDEN, f"an AI agent session cannot {action}",
                next_action="Ask the person who started the session to do this.",
            )

    # -- authorization -----------------------------------------------------------

    @staticmethod
    def require(principal: Principal, organization_id: str, minimum: str, what: str) -> None:
        """Refuse unless the principal holds at least ``minimum`` in the organization.

        Callers outside the organization get NOT_FOUND so identifiers do not
        reveal whether an object exists.
        """
        role = principal.role_in(organization_id)
        if role is None:
            raise PlatformError(ErrorCode.NOT_FOUND, f"{what} was not found")
        if ROLE_RANK[role] < ROLE_RANK[minimum]:
            raise PlatformError(
                ErrorCode.FORBIDDEN, f"this action on {what} needs the {minimum} role"
            )

    def owned(
        self,
        s: Session,
        model: type[T],
        object_id: str,
        principal: Principal,
        minimum: str,
        *,
        lock: bool = False,
    ) -> T:
        obj = s.get(model, object_id, with_for_update=lock)
        what = f"{model.__name__.lower()} {object_id}"
        if obj is None:
            raise PlatformError(ErrorCode.NOT_FOUND, f"{what} was not found")
        self.require(principal, obj.organization_id, minimum, what)  # type: ignore[attr-defined]
        return obj

    @staticmethod
    def audit(
        s: Session,
        organization_id: str | None,
        actor: dict[str, Any],
        action: str,
        object_type: str,
        object_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        s.add(
            AuditRecord(
                organization_id=organization_id, actor=actor, action=action,
                object_type=object_type, object_id=object_id, details=details,
            )
        )


    # -- shared state helpers ---------------------------------------------------

    def _set_state(self, job: Job, state: JobState, failure: dict | None = None) -> None:
        """Move a job along an allowed transition; anything else is a bug, not a race."""
        current = JobState(job.state)
        if not job_can(current, state):
            raise PlatformError(ErrorCode.INVALID_TRANSITION,
                                f"job {job.id} cannot move from {current.value} to {state.value}")
        job.state = state.value
        job.version += 1
        job.updated_at = self.now()
        if failure is not None:
            job.failure = failure
        if state in JOB_TERMINAL:
            job.terminal_at = self.now()

    def _grant_request(self, s: Session, organization_id: str, target_id: str,
                       principal: Principal, mode: GrantMode, objects: list[dict],
                       revision_id: str | None = None, run_id: str | None = None) -> str:
        row = GrantRequestRow(
            id=new_id("grq"), organization_id=organization_id, target_id=target_id,
            revision_id=revision_id, run_id=run_id, mode=mode.value, objects=objects,
            requested_by=principal.user_id, expires_at=self.now() + timedelta(hours=1),
        )
        s.add(row)
        return row.id


def human_actor(principal: Principal) -> dict[str, Any]:
    return {"human_subject": principal.subject, "user_id": principal.user_id}
