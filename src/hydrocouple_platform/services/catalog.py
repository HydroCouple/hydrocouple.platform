"""Organizations, members, projects, sessions, compute targets and agent sessions."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from ..contracts.errors import ErrorCode, PlatformError
from ..contracts.requests import AgentSessionCreate, TargetCreate
from ..db.models import (
    AgentSession,
    ComputeTarget,
    Membership,
    Organization,
    PlatformSession,
    Project,
    User,
    new_id,
)
from .auth import (
    SESSION_CREDENTIAL_PREFIX,
    Principal,
    credential_digest,
    new_agent_credential,
)
from .base import ServiceBase, human_actor

KNOWN_ADAPTERS = {"local-pool", "kubernetes", "aws-batch", "azure-batch", "gcp-batch",
                  "cloud-run-jobs", "container-apps-jobs", "ecs-fargate", "slurm"}


class CatalogServices(ServiceBase):
    # -- bootstrap (operator CLI only; no principal) ---------------------------

    def bootstrap_organization(
        self, name: str, admin_issuer: str, admin_subject: str, admin_email: str | None = None
    ) -> str:
        with self.tx() as s:
            user = s.scalar(
                select(User).where(User.issuer == admin_issuer, User.subject == admin_subject)
            )
            if user is None:
                user = User(id=new_id("usr"), issuer=admin_issuer, subject=admin_subject,
                            email=admin_email)
                s.add(user)
            org = Organization(id=new_id("org"), name=name)
            s.add(org)
            s.flush()
            s.add(Membership(organization_id=org.id, user_id=user.id, role="admin"))
            self.audit(s, org.id, {"operator": "bootstrap"}, "organization.create",
                       "organization", org.id, {"admin_subject": admin_subject})
            return org.id

    # -- members -----------------------------------------------------------------

    def add_member(
        self, principal: Principal, organization_id: str, issuer: str, subject: str, role: str
    ) -> None:
        if role not in ("admin", "editor", "viewer"):
            raise PlatformError(ErrorCode.VALIDATION_ERROR, f"unknown role {role!r}")
        self.human_only(principal, "change membership")
        self.require(principal, organization_id, "admin", f"organization {organization_id}")
        with self.tx() as s:
            user = s.scalar(select(User).where(User.issuer == issuer, User.subject == subject))
            if user is None:
                user = User(id=new_id("usr"), issuer=issuer, subject=subject)
                s.add(user)
                s.flush()
            existing = s.get(Membership, (organization_id, user.id))
            if existing:
                existing.role = role
            else:
                s.add(Membership(organization_id=organization_id, user_id=user.id, role=role))
            self.audit(s, organization_id, human_actor(principal), "member.set", "user", user.id,
                       {"role": role})

    # -- projects and sessions ---------------------------------------------------

    def create_project(self, principal: Principal, organization_id: str, name: str) -> dict:
        self.require(principal, organization_id, "editor", f"organization {organization_id}")
        with self.tx() as s:
            project = Project(id=new_id("prj"), organization_id=organization_id, name=name)
            s.add(project)
            try:
                s.flush()
            except IntegrityError:
                raise PlatformError(
                    ErrorCode.CONFLICT, f"a project named {name!r} already exists"
                ) from None
            self.audit(s, organization_id, human_actor(principal), "project.create", "project",
                       project.id)
            return {"project_id": project.id, "organization_id": organization_id, "name": name}

    def create_session(self, principal: Principal, project_id: str, name: str) -> dict:
        with self.tx() as s:
            project = self.owned(s, Project, project_id, principal, "editor")
            row = PlatformSession(id=new_id("ses"), organization_id=project.organization_id,
                                  project_id=project.id, name=name, created_by=principal.user_id)
            s.add(row)
            return {"platform_session_id": row.id, "project_id": project.id, "name": name}

    def list_sessions(self, principal: Principal, project_id: str) -> list[dict]:
        with self.tx() as s:
            self.owned(s, Project, project_id, principal, "viewer")
            rows = s.scalars(
                select(PlatformSession).where(PlatformSession.project_id == project_id)
                .order_by(PlatformSession.created_at.desc()).limit(200)
            )
            return [{"platform_session_id": r.id, "name": r.name,
                     "created_at": r.created_at.isoformat()} for r in rows]

    # -- compute targets ---------------------------------------------------------

    def register_target(
        self, principal: Principal, organization_id: str, body: TargetCreate
    ) -> dict[str, Any]:
        """Register a target and return its agent credential, which is shown only once."""
        self.human_only(principal, "register compute targets")
        self.require(principal, organization_id, "admin", f"organization {organization_id}")
        if body.adapter not in KNOWN_ADAPTERS:
            raise PlatformError(ErrorCode.VALIDATION_ERROR, f"unknown adapter {body.adapter!r}")
        token, digest = new_agent_credential()
        with self.tx() as s:
            target = ComputeTarget(
                id=new_id("tgt"), organization_id=organization_id, name=body.name,
                ownership_mode=body.ownership_mode, adapter=body.adapter, region=body.region,
                storage_uri=body.storage_uri, egress_policy=body.egress_policy.value,
                credential_hash=digest,
            )
            s.add(target)
            try:
                s.flush()
            except IntegrityError:
                raise PlatformError(
                    ErrorCode.CONFLICT, f"a target named {body.name!r} already exists"
                ) from None
            self.audit(s, organization_id, human_actor(principal), "target.register", "target",
                       target.id, {"adapter": body.adapter})
            return {"target_id": target.id, "agent_credential": token}

    def revoke_target(self, principal: Principal, target_id: str) -> None:
        self.human_only(principal, "revoke compute targets")
        with self.tx() as s:
            target = self.owned(s, ComputeTarget, target_id, principal, "admin", lock=True)
            if target.revoked_at is None:
                target.revoked_at = self.now()
                self.audit(s, target.organization_id, human_actor(principal), "target.revoke",
                           "target", target.id)

    def list_targets(self, principal: Principal, organization_id: str) -> list[dict]:
        self.require(principal, organization_id, "viewer", f"organization {organization_id}")
        with self.tx() as s:
            rows = s.scalars(select(ComputeTarget).where(
                ComputeTarget.organization_id == organization_id))
            return [
                {
                    "target_id": t.id, "name": t.name, "adapter": t.adapter,
                    "ownership_mode": t.ownership_mode, "egress_policy": t.egress_policy,
                    "revoked": t.revoked_at is not None, "capabilities": t.capabilities,
                    "last_poll_at": t.last_poll_at.isoformat() if t.last_poll_at else None,
                }
                for t in rows
            ]

    def agent_target(self, credential: str, *, allow_revoked: bool = False) -> ComputeTarget:
        """Authenticate an agent by its target credential.

        A revoked target may still report on attempts it already holds
        (``allow_revoked``), so in-flight work drains; it may not claim new work.
        """
        with self.tx() as s:
            target = s.scalar(
                select(ComputeTarget).where(
                    ComputeTarget.credential_hash == credential_digest(credential))
            )
            if target is None:
                raise PlatformError(ErrorCode.UNAUTHENTICATED, "unknown agent credential")
            if target.revoked_at is not None and not allow_revoked:
                raise PlatformError(
                    ErrorCode.TARGET_REVOKED,
                    "this compute target was revoked; the agent may not claim work",
                    next_action="Let in-flight attempts finish, then stop the agent.",
                )
            s.expunge(target)
            return target

    # -- AI agent sessions -------------------------------------------------------

    def create_agent_session(self, principal: Principal, body: AgentSessionCreate) -> dict:
        """Start a delegated session; its credential is returned once, for the AI client."""
        self.human_only(principal, "start another agent session")
        token, digest = new_agent_credential(SESSION_CREDENTIAL_PREFIX)
        with self.tx() as s:
            project = self.owned(s, Project, body.project_id, principal, "editor")
            row = AgentSession(
                id=new_id("ags"), organization_id=project.organization_id, project_id=project.id,
                created_by=principal.user_id, client_name=body.client_name,
                client_version=body.client_version, model=body.model,
                conversation_ref=body.conversation_ref,
                budget=body.budget.model_dump(mode="json"), credential_hash=digest,
                expires_at=self.now() + timedelta(seconds=body.expires_in_seconds),
            )
            s.add(row)
            self.audit(s, project.organization_id, human_actor(principal), "agent_session.create",
                       "agent_session", row.id, {"budget": row.budget})
            return {"agent_session_id": row.id, "agent_session_token": token,
                    "expires_at": row.expires_at.isoformat(), "budget": row.budget}

    def revoke_agent_session(self, principal: Principal, agent_session_id: str) -> None:
        self.human_only(principal, "revoke agent sessions")
        with self.tx() as s:
            row = self.owned(s, AgentSession, agent_session_id, principal, "editor", lock=True)
            if row.revoked_at is None:
                row.revoked_at = self.now()
                self.audit(s, row.organization_id, human_actor(principal),
                           "agent_session.revoke", "agent_session", row.id)

    def session_principal(self, token: str) -> Principal:
        """The person an agent session acts for, carrying the session's identity."""
        with self.tx() as s:
            row = s.scalar(select(AgentSession).where(
                AgentSession.credential_hash == credential_digest(token)))
            if row is None:
                raise PlatformError(ErrorCode.UNAUTHENTICATED, "unknown agent session credential")
            if row.revoked_at is not None or row.expires_at <= self.now():
                raise PlatformError(ErrorCode.UNAUTHENTICATED,
                                    "the agent session expired or was revoked",
                                    next_action="Ask the user to start a new agent session.")
            return self._principal(s, s.get(User, row.created_by), agent_session_id=row.id)
