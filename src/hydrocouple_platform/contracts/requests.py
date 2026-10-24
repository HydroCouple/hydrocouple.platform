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
    max_concurrent_attempts: int | None = Field(default=None, ge=0)
    max_estimated_spend: float | None = Field(default=None, ge=0)
    allowed_targets: list[str] | None = None
    allowed_job_classes: list[JobClass] | None = None


class AgentSessionCreate(_Model):
    project_id: str
    client_name: str
    client_version: str | None = None
    model: str | None = None
    conversation_ref: str | None = None
    budget: Budget = Field(default_factory=Budget)
    expires_in_seconds: int = Field(default=8 * 3600, gt=0, le=30 * 86400)


class TargetCreate(_Model):
    name: str = Field(min_length=1, max_length=120)
    ownership_mode: str = Field(pattern="^(organization-owned|service-managed)$")
    adapter: str
    storage_uri: str
    region: str | None = None
    egress_policy: EgressPolicy = EgressPolicy.SUMMARIES


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


class Estimate(_Model):
    ok: bool
    findings: list[str]
    job_class: JobClass | None = None
    profile: str | None = None
    evaluations: int = 1
    cost_band: str = "unknown"
    requires_approval: bool = False
    approval_reasons: list[str] = Field(default_factory=list)


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
