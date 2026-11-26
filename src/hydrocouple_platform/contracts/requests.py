"""Client-facing request and response bodies (GUI, CLI, SDK, MCP)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .jobs import EgressPolicy, JobClass
from .manifests import RevisionManifest


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Budget(_Model):
    """A delegated budget for one AI agent session. ``None`` means no limit."""

    max_runs: int | None = Field(default=None, ge=0)
    # Study evaluations across the session; each study reserves its own budget.
    max_evaluations: int | None = Field(default=None, ge=0)
    max_concurrent_attempts: int | None = Field(default=None, ge=0)
    max_estimated_spend: float | None = Field(default=None, ge=0)
    # How long the session's work may run, from its start; past it the reconciler cancels
    # its active runs and studies and it takes no new work, but its results stay readable.
    max_wall_time_seconds: int | None = Field(default=None, gt=0)
    allowed_targets: list[str] | None = None
    allowed_job_classes: list[JobClass] | None = None


class AgentSessionCreate(_Model):
    project_id: str
    client_name: str
    client_version: str | None = None
    model: str | None = None
    # The person's conversation with the AI client, so its work traces back to what was
    # asked; required since M7 (sessions opened before keep none).
    conversation_ref: str = Field(min_length=1, max_length=200)
    budget: Budget = Field(default_factory=Budget)
    expires_in_seconds: int = Field(default=8 * 3600, gt=0, le=30 * 86400)


class AgentSessionView(_Model):
    """An agent session as its creator sees it (the credential is returned only once)."""

    agent_session_id: str
    expires_at: datetime
    budget: Budget
    used: dict[str, float] | None = Field(
        default=None, description="runs submitted, evaluations reserved by its studies, "
                                  "attempts and pool tasks active, estimated spend committed")
    remaining: dict[str, float | None] | None = Field(
        default=None, description="what the budget leaves of each; null where it sets no limit")


class OrganizationPolicy(_Model):
    """Rules an organization's admins set for every submission in it."""

    require_estimate_above_evaluations: int | None = Field(
        default=None, ge=0,
        description="a run or study above this many evaluations must name a matching, "
                    "unexpired estimate (estimate_id); null means never required")


class PriceTable(_Model):
    """A target's price source: the price of one hour of each resource profile."""

    currency: str = Field(pattern="^[A-Z]{3}$")
    per_hour: dict[str, float] = Field(description="resource profile name to price per hour")
    as_of: datetime | None = None


class ApprovalView(_Model):
    """A request outside an agent session's budget, waiting for a person."""

    approval_id: str
    run_id: str | None = None
    job_id: str | None = None
    study_id: str | None = None
    reasons: list[str]
    expires_at: datetime


class TargetCreate(_Model):
    name: str = Field(min_length=1, max_length=120)
    ownership_mode: str = Field(pattern="^(organization-owned|service-managed)$")
    adapter: str
    storage_uri: str
    region: str | None = None
    egress_policy: EgressPolicy = EgressPolicy.SUMMARIES
    price_table: PriceTable | None = None


class RevisionCreate(_Model):
    project_id: str
    session_id: str | None = None
    target_id: str
    manifest: RevisionManifest


class RunRequest(_Model):
    project_id: str
    session_id: str | None = None
    revision_id: str
    workflow: str
    workflow_version: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    target_id: str
    profile: str | None = None
    retry_budget: int | None = Field(default=None, ge=0, le=10)
    estimated_runtime_seconds: float | None = Field(default=None, gt=0)
    tool_call_id: str | None = None
    estimate_id: str | None = Field(
        default=None, description="The estimate this submission follows; required above the "
                                  "organization's require_estimate_above_evaluations")
    resume_from: str | None = Field(
        default=None, description="A published run of the same resumable workflow, project and "
                                  "target whose saved state this run continues from")


class Estimate(_Model):
    ok: bool
    findings: list[str]
    job_class: JobClass | None = None
    profile: str | None = None
    evaluations: int = 1
    pool_size: int | None = Field(default=None, description="a study's concurrent pool tasks")
    estimated_spend: float | None = Field(
        default=None, description="from the target's price table and the stated runtime; "
                                  "null when either is missing")
    currency: str | None = None
    cost_band: str = "unknown"
    requires_approval: bool = False
    approval_reasons: list[str] = Field(default_factory=list)
    estimate_id: str | None = Field(
        default=None, description="name it in the submission (estimate_id) when the "
                                  "organization requires an estimate for one this size")
    expires_at: datetime | None = None


class RunAccepted(_Model):
    platform_session_id: str | None
    run_id: str
    job_id: str
    status: str
    job_class: JobClass
    model_revision_id: str
    compute_target_id: str
    approval_id: str | None = None
    budget_remaining: dict[str, Any] | None = None
    next_action: str


class JobView(_Model):
    job_id: str
    run_id: str
    state: str
    cancel_requested: bool
    agent_unreachable: bool
    attempts_used: int
    retry_budget: int
    target_id: str
    created_at: datetime
    updated_at: datetime
    failure: dict[str, Any] | None = None
    progress: dict[str, Any] | None = None
    attempts: list[dict[str, Any]] = Field(default_factory=list)
