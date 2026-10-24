"""The versioned job specification an agent receives with each lease.

The spec is self-contained: an agent and runner can execute it without any
other platform state. It never contains user bearer tokens or credentials.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JOB_SPEC_VERSION = "1"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class JobClass(str, Enum):
    SINGLE = "single"
    EVALUATOR_POOL = "evaluator-pool"
    MULTI_RANK = "multi-rank"
    ANALYSIS = "analysis"


class EgressPolicy(str, Enum):
    """What an agent lets leave the execution site in events and results."""

    METRICS_ONLY = "metrics-only"
    SUMMARIES = "summaries"
    FULL = "full"


class FailureClass(str, Enum):
    """Evidence-based failure classes; each maps to one retry rule."""

    INVALID_INPUT = "invalid_input"
    MISSING_DEPENDENCY = "missing_dependency"
    OUT_OF_MEMORY = "out_of_memory"
    ENGINE_CRASH = "engine_crash"
    INTERRUPTED = "interrupted"
    CAPACITY = "capacity"
    TRANSFER = "transfer"
    TIMEOUT = "timeout"
    LOST = "lost"
    UNKNOWN = "unknown"


# How many times a failure of each class may be retried, regardless of the job's
# retry budget (an engine crash is retried once, on a fresh attempt, then fails).
# Deterministic failures never retry.
FAILURE_RETRIES: dict[FailureClass, int] = {
    FailureClass.INVALID_INPUT: 0,
    FailureClass.MISSING_DEPENDENCY: 0,
    FailureClass.OUT_OF_MEMORY: 1,
    FailureClass.ENGINE_CRASH: 1,
    FailureClass.INTERRUPTED: 1_000,
    FailureClass.CAPACITY: 1_000,
    FailureClass.TRANSFER: 1_000,
    FailureClass.TIMEOUT: 0,
    FailureClass.LOST: 1_000,
    FailureClass.UNKNOWN: 1,
}


class Actor(_Model):
    """Who asked for the work: the human principal and, optionally, an AI agent."""

    human_subject: str
    agent_session_id: str | None = None
    agent_client: str | None = None
    agent_client_version: str | None = None
    model: str | None = None
    conversation_ref: str | None = None
    tool_call_id: str | None = None


class Workload(_Model):
    workflow: str
    workflow_version: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class Runtime(_Model):
    # A signed image digest for container adapters. None means the workflow is
    # qualified only for the local pool, which runs the installed runner package.
    image: str | None = None
    architecture: str | None = None
    threads: int = Field(default=1, ge=1)


class InputFile(_Model):
    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)


class Inputs(_Model):
    revision_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    storage_prefix: str
    composition: str = "composition.json"
    # Every file of the revision; the runner checks each one before running.
    files: list[InputFile] = Field(min_length=1)


class Resources(_Model):
    job_class: JobClass = JobClass.SINGLE
    profile: str = "cpu-small"
    cpu: float = Field(default=1.0, gt=0)
    memory_gib: float = Field(default=2.0, gt=0)
    gpu: str | None = None
    scratch_gib: float = Field(default=4.0, ge=0)
    ranks: int = Field(default=1, ge=1)
    pool_size: int = Field(default=1, ge=1)


class Placement(_Model):
    target_id: str
    interruption: Literal["standard", "interruptible"] = "standard"


class Limits(_Model):
    queue_deadline_seconds: int = Field(default=24 * 3600, gt=0)
    execution_timeout_seconds: int = Field(default=6 * 3600, gt=0)
    retry_budget: int = Field(default=2, ge=0)
    estimated_runtime_seconds: float | None = None


class Outputs(_Model):
    storage_prefix: str
    required_artifacts: list[str]
    egress_policy: EgressPolicy = EgressPolicy.SUMMARIES
    retention_days: int | None = None


class JobSpec(_Model):
    spec_version: Literal["1"] = JOB_SPEC_VERSION
    job_id: str
    run_id: str
    organization_id: str
    project_id: str
    actor: Actor
    workload: Workload
    runtime: Runtime
    inputs: Inputs
    resources: Resources
    placement: Placement
    limits: Limits
    outputs: Outputs
    recovery: Literal["restart"] = "restart"
