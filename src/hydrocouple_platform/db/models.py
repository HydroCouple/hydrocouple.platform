"""Authoritative control-plane records (PostgreSQL).

Ownership and lifecycle live in relational columns with constraints; workflow
specific content lives in versioned JSON. Per-evaluation and per-timestep data
never become rows: they live in objects indexed by manifests.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..contracts.states import AttemptState, JobState


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    """Opaque identifiers; ownership is always checked, never inferred from them."""
    return f"{prefix}_{secrets.token_hex(10)}"


class Base(DeclarativeBase):
    pass


def _states_check(enum) -> str:
    return "state in (" + ",".join(f"'{m.value}'" for m in enum) + ")"


def _ts(nullable: bool = False, default: bool = True):
    return mapped_column(
        DateTime(timezone=True), nullable=nullable, default=utcnow if default else None
    )


class Organization(Base):
    __tablename__ = "organizations"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    created_at: Mapped[datetime] = _ts()


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    issuer: Mapped[str] = mapped_column(String(400))
    subject: Mapped[str] = mapped_column(String(400))
    email: Mapped[str | None] = mapped_column(String(400))
    created_at: Mapped[datetime] = _ts()
    __table_args__ = (UniqueConstraint("issuer", "subject"),)


class Membership(Base):
    __tablename__ = "memberships"
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    role: Mapped[str] = mapped_column(String(20))
    __table_args__ = (CheckConstraint("role in ('admin','editor','viewer')", name="role_known"),)


class Project(Base):
    __tablename__ = "projects"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    default_target_id: Mapped[str | None] = mapped_column(String(40))
    created_at: Mapped[datetime] = _ts()
    __table_args__ = (UniqueConstraint("organization_id", "name"),)


class PlatformSession(Base):
    __tablename__ = "platform_sessions"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    created_by: Mapped[str] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = _ts()


class ComputeTarget(Base):
    __tablename__ = "compute_targets"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    name: Mapped[str] = mapped_column(String(120))
    ownership_mode: Mapped[str] = mapped_column(String(40))
    adapter: Mapped[str] = mapped_column(String(60))
    region: Mapped[str | None] = mapped_column(String(80))
    storage_uri: Mapped[str] = mapped_column(Text)
    egress_policy: Mapped[str] = mapped_column(String(20))
    credential_hash: Mapped[str] = mapped_column(String(64), unique=True)
    capabilities: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    agent_instance: Mapped[str | None] = mapped_column(String(128))
    agent_version: Mapped[str | None] = mapped_column(String(40))
    last_poll_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    revoked_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    created_at: Mapped[datetime] = _ts()
    __table_args__ = (UniqueConstraint("organization_id", "name"),)


class Revision(Base):
    __tablename__ = "revisions"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    session_id: Mapped[str | None] = mapped_column(ForeignKey("platform_sessions.id"))
    target_id: Mapped[str] = mapped_column(ForeignKey("compute_targets.id"))
    state: Mapped[str] = mapped_column(String(20))
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB)
    manifest_sha256: Mapped[str] = mapped_column(String(64))
    storage_prefix: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = _ts()
    finalized_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    __table_args__ = (
        CheckConstraint("state in ('UPLOADING','VERIFYING','READY','FAILED')", name="state_known"),
    )


class GrantRequestRow(Base):
    __tablename__ = "grant_requests"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    target_id: Mapped[str] = mapped_column(ForeignKey("compute_targets.id"), index=True)
    revision_id: Mapped[str | None] = mapped_column(ForeignKey("revisions.id"))
    run_id: Mapped[str | None] = mapped_column(String(40))
    mode: Mapped[str] = mapped_column(String(20))
    objects: Mapped[list[Any]] = mapped_column(JSONB)
    state: Mapped[str] = mapped_column(String(20), default="PENDING")
    grants: Mapped[list[Any] | None] = mapped_column(JSONB)
    problems: Mapped[list[Any] | None] = mapped_column(JSONB)
    requested_by: Mapped[str] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = _ts()
    delivered_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    fulfilled_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    expires_at: Mapped[datetime] = _ts()


class AgentSession(Base):
    __tablename__ = "agent_sessions"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    created_by: Mapped[str] = mapped_column(ForeignKey("users.id"))
    client_name: Mapped[str] = mapped_column(String(120))
    client_version: Mapped[str | None] = mapped_column(String(60))
    model: Mapped[str | None] = mapped_column(String(120))
    conversation_ref: Mapped[str | None] = mapped_column(String(200))
    budget: Mapped[dict[str, Any]] = mapped_column(JSONB)
    credential_hash: Mapped[str] = mapped_column(String(64), unique=True)
    runs_submitted: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = _ts()
    expires_at: Mapped[datetime] = _ts()
    revoked_at: Mapped[datetime | None] = _ts(nullable=True, default=False)


class Run(Base):
    __tablename__ = "runs"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    session_id: Mapped[str | None] = mapped_column(ForeignKey("platform_sessions.id"), index=True)
    revision_id: Mapped[str] = mapped_column(ForeignKey("revisions.id"))
    target_id: Mapped[str] = mapped_column(ForeignKey("compute_targets.id"))
    workflow: Mapped[str] = mapped_column(String(120))
    workflow_version: Mapped[str] = mapped_column(String(20))
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB)
    job_class: Mapped[str] = mapped_column(String(20))
    idempotency_key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    actor: Mapped[dict[str, Any]] = mapped_column(JSONB)
    agent_session_id: Mapped[str | None] = mapped_column(ForeignKey("agent_sessions.id"))
    created_by: Mapped[str] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = _ts()
    canonical_attempt_id: Mapped[str | None] = mapped_column(String(40))
    published_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    summary: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    __table_args__ = (
        UniqueConstraint("organization_id", "idempotency_key", name="runs_idempotency"),
        Index("runs_history", "organization_id", "project_id", "created_at"),
    )


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    target_id: Mapped[str] = mapped_column(ForeignKey("compute_targets.id"))
    state: Mapped[str] = mapped_column(String(20))
    spec: Mapped[dict[str, Any]] = mapped_column(JSONB)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    agent_unreachable: Mapped[bool] = mapped_column(Boolean, default=False)
    current_attempt_id: Mapped[str | None] = mapped_column(String(40))
    attempts_used: Mapped[int] = mapped_column(Integer, default=0)
    retry_budget: Mapped[int] = mapped_column(Integer)
    failure: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    progress: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    # Evaluator-pool task jobs belong to a study; a retired task takes no more work.
    study_id: Mapped[str | None] = mapped_column(
        ForeignKey("studies.id", name="jobs_study_fk"), index=True)
    retired_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    version: Mapped[int] = mapped_column(Integer, default=1)
    leasable_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    queue_deadline_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    created_at: Mapped[datetime] = _ts()
    updated_at: Mapped[datetime] = _ts()
    terminal_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    __table_args__ = (
        CheckConstraint(_states_check(JobState), name="job_state_known"),
        CheckConstraint("attempts_used >= 0 and retry_budget >= 0", name="job_counts"),
        Index("jobs_leasable", "target_id", "state", "leasable_at"),
        Index("jobs_status", "organization_id", "state"),
    )


class Attempt(Base):
    __tablename__ = "attempts"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), index=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"))
    target_id: Mapped[str] = mapped_column(ForeignKey("compute_targets.id"))
    number: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(20))
    agent_instance: Mapped[str] = mapped_column(String(128))
    provider_reference: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime] = _ts()
    execution_deadline_at: Mapped[datetime] = _ts()
    last_heartbeat_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    started_at: Mapped[datetime] = _ts()
    finished_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    failure_class: Mapped[str | None] = mapped_column(String(40))
    exit_code: Mapped[int | None] = mapped_column(Integer)
    diagnostics: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    manifest: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    last_sequence: Mapped[int] = mapped_column(Integer, default=-1)
    __table_args__ = (
        CheckConstraint(_states_check(AttemptState), name="attempt_state_known"),
        UniqueConstraint("job_id", "number", name="attempt_fence_unique"),
        Index("attempts_expiry", "state", "lease_expires_at"),
        Index("attempts_deadline", "state", "execution_deadline_at"),
    )


class EventRow(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64))
    organization_id: Mapped[str] = mapped_column(String(40), index=True)
    job_id: Mapped[str] = mapped_column(String(40), index=True)
    attempt_id: Mapped[str] = mapped_column(String(40))
    sequence: Mapped[int] = mapped_column(Integer)
    kind: Mapped[str] = mapped_column(String(20))
    phase: Mapped[str | None] = mapped_column(String(60))
    completed: Mapped[float | None] = mapped_column(Float)
    total: Mapped[float | None] = mapped_column(Float)
    unit: Mapped[str | None] = mapped_column(String(40))
    message: Mapped[str | None] = mapped_column(Text)
    agent_clock_seconds: Mapped[float | None] = mapped_column(Float)
    occurred_at: Mapped[datetime] = _ts()
    received_at: Mapped[datetime] = _ts()
    __table_args__ = (UniqueConstraint("attempt_id", "event_id", name="events_dedupe"),)


class Artifact(Base):
    __tablename__ = "artifacts"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    attempt_id: Mapped[str] = mapped_column(ForeignKey("attempts.id"))
    path: Mapped[str] = mapped_column(Text)
    storage_key: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(String(30))
    format: Mapped[str] = mapped_column(String(40))
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = _ts()
    __table_args__ = (UniqueConstraint("attempt_id", "path"),)


class Approval(Base):
    __tablename__ = "approvals"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"))
    # A run approval names its job; a study approval names its study instead.
    job_id: Mapped[str | None] = mapped_column(ForeignKey("jobs.id"))
    study_id: Mapped[str | None] = mapped_column(
        ForeignKey("studies.id", name="approvals_study_fk"))
    agent_session_id: Mapped[str | None] = mapped_column(ForeignKey("agent_sessions.id"))
    reasons: Mapped[list[Any]] = mapped_column(JSONB)
    state: Mapped[str] = mapped_column(String(20), default="PENDING")
    requested_at: Mapped[datetime] = _ts()
    expires_at: Mapped[datetime] = _ts()
    resolved_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"))
    resolved_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    note: Mapped[str | None] = mapped_column(Text)


class Study(Base):
    """A registered evaluation function; its evaluator pool tasks are jobs of ``run_id``."""

    __tablename__ = "studies"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    session_id: Mapped[str | None] = mapped_column(ForeignKey("platform_sessions.id"))
    revision_id: Mapped[str] = mapped_column(ForeignKey("revisions.id"))
    target_id: Mapped[str] = mapped_column(ForeignKey("compute_targets.id"))
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), unique=True)
    state: Mapped[str] = mapped_column(String(20))
    # Dimensions, objectives, seed, failed-candidate policy and pool task template.
    definition: Mapped[dict[str, Any]] = mapped_column(JSONB)
    max_evaluations: Mapped[int] = mapped_column(Integer)
    pool_size: Mapped[int] = mapped_column(Integer)
    batches_submitted: Mapped[int] = mapped_column(Integer, default=0)
    evaluations_submitted: Mapped[int] = mapped_column(Integer, default=0)
    evaluations_completed: Mapped[int] = mapped_column(Integer, default=0)
    evaluations_failed: Mapped[int] = mapped_column(Integer, default=0)
    idempotency_key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    actor: Mapped[dict[str, Any]] = mapped_column(JSONB)
    agent_session_id: Mapped[str | None] = mapped_column(ForeignKey("agent_sessions.id"))
    created_by: Mapped[str] = mapped_column(ForeignKey("users.id"))
    failure: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _ts()
    updated_at: Mapped[datetime] = _ts()
    terminal_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    __table_args__ = (
        CheckConstraint("state in ('PENDING_APPROVAL','OPEN','CLOSED','COMPLETED','CANCELLED',"
                        "'FAILED','REJECTED')", name="study_state_known"),
        CheckConstraint("evaluations_submitted <= max_evaluations", name="study_budget"),
        UniqueConstraint("organization_id", "idempotency_key", name="studies_idempotency"),
    )


class StudyBatch(Base):
    __tablename__ = "study_batches"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    study_id: Mapped[str] = mapped_column(ForeignKey("studies.id"), index=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"))
    sequence: Mapped[int] = mapped_column(Integer)
    # Evaluations submitted before this batch; candidate i is evaluation offset + i + 1.
    offset: Mapped[int] = mapped_column(Integer)
    size: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    candidates: Mapped[list[Any]] = mapped_column(JSONB)
    state: Mapped[str] = mapped_column(String(20))
    completed: Mapped[int] = mapped_column(Integer, default=0)
    results: Mapped[list[Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _ts()
    completed_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    __table_args__ = (
        CheckConstraint("state in ('QUEUED','DONE','CANCELLED','FAILED')",
                        name="batch_state_known"),
        UniqueConstraint("study_id", "idempotency_key", name="batches_idempotency"),
        UniqueConstraint("study_id", "sequence", name="batches_sequence"),
    )


class EvaluationChunk(Base):
    """The unit of an evaluation lease: candidates ``start..stop`` of one batch.

    ``lease_number`` is the fence: a completion is accepted only at the current
    number from the attempt that holds the chunk.
    """

    __tablename__ = "evaluation_chunks"
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    study_id: Mapped[str] = mapped_column(ForeignKey("studies.id"))
    batch_id: Mapped[str] = mapped_column(ForeignKey("study_batches.id"), index=True)
    organization_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"))
    batch_sequence: Mapped[int] = mapped_column(Integer)
    start: Mapped[int] = mapped_column(Integer)
    stop: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(20), default="PENDING")
    lease_number: Mapped[int] = mapped_column(Integer, default=0)
    deliveries: Mapped[int] = mapped_column(Integer, default=0)
    holder_attempt_id: Mapped[str | None] = mapped_column(String(40))
    lease_expires_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    results: Mapped[list[Any] | None] = mapped_column(JSONB)
    object_key: Mapped[str | None] = mapped_column(Text)
    object_sha256: Mapped[str | None] = mapped_column(String(64))
    object_size: Mapped[int | None] = mapped_column(BigInteger)
    completed_attempt_id: Mapped[str | None] = mapped_column(String(40))
    completed_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    __table_args__ = (
        CheckConstraint("state in ('PENDING','LEASED','DONE','CANCELLED')",
                        name="chunk_state_known"),
        CheckConstraint("start >= 0 and stop > start", name="chunk_range"),
        Index("chunks_queue", "study_id", "state", "batch_sequence", "start"),
        Index("chunks_expiry", "state", "lease_expires_at"),
        Index("chunks_holder", "holder_attempt_id"),
    )


class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(60))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _ts()
    available_at: Mapped[datetime] = _ts()
    tries: Mapped[int] = mapped_column(Integer, default=0)
    processed_at: Mapped[datetime | None] = _ts(nullable=True, default=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    __table_args__ = (Index("outbox_pending", "processed_at", "available_at"),)


class AuditRecord(Base):
    __tablename__ = "audit"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    organization_id: Mapped[str | None] = mapped_column(String(40), index=True)
    actor: Mapped[dict[str, Any]] = mapped_column(JSONB)
    action: Mapped[str] = mapped_column(String(80))
    object_type: Mapped[str] = mapped_column(String(40))
    object_id: Mapped[str] = mapped_column(String(40))
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    at: Mapped[datetime] = _ts()
