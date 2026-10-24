"""The protocol between an execution agent and the control plane.

The agent always opens the connection. It polls for leases, grant requests and
cancellations; it reports heartbeats, events and results for attempts it holds.
Every attempt report carries the attempt token and fencing token issued with
the lease, and the control plane refuses reports from a superseded attempt.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .capabilities import AdapterCapabilities
from .jobs import FailureClass, JobSpec
from .states import AttemptState


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EventKind(str, Enum):
    LIFECYCLE = "lifecycle"
    PROGRESS = "progress"
    WARNING = "warning"
    LOG = "log"


class Event(_Model):
    event_id: str = Field(min_length=8, max_length=64)
    sequence: int = Field(ge=0)
    kind: EventKind
    phase: str | None = None
    completed: float | None = None
    total: float | None = None
    unit: str | None = None
    message: str | None = Field(default=None, max_length=4000)
    occurred_at: datetime
    agent_clock_seconds: float | None = Field(
        default=None, description="Agent monotonic clock, robust to wall-clock skew."
    )


class PollRequest(_Model):
    agent_instance: str = Field(min_length=1, max_length=128)
    agent_version: str
    capabilities: AdapterCapabilities
    free_slots: int = Field(ge=0)
    wait_seconds: float = Field(default=0, ge=0, le=30)
    # Attempts this agent instance is running; the control plane answers with the
    # ones to stop and re-offers any lease it issued that the agent never received.
    held_attempts: list[str] = Field(default_factory=list, max_length=1000)


class LeaseGrant(_Model):
    attempt_id: str
    job_id: str
    fencing_token: int = Field(ge=1)
    attempt_token: str
    lease_expires_at: datetime
    heartbeat_interval_seconds: float
    spec: JobSpec


class GrantMode(str, Enum):
    UPLOAD = "upload"
    DOWNLOAD = "download"
    VERIFY = "verify"


class ObjectRef(_Model):
    key: str
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    size: int | None = Field(default=None, ge=0)


class GrantRequest(_Model):
    grant_request_id: str
    mode: GrantMode
    objects: list[ObjectRef]
    expires_in_seconds: int = 900


class Grant(_Model):
    key: str
    method: Literal["file", "PUT", "GET"]
    url: str
    headers: dict[str, str] = Field(default_factory=dict)
    expires_at: datetime | None = None


class GrantFulfillment(_Model):
    grant_request_id: str
    ok: bool
    grants: list[Grant] = Field(default_factory=list)
    # For VERIFY: keys that were missing or whose checksum or size did not match.
    problems: list[str] = Field(default_factory=list)


class PollResponse(_Model):
    leases: list[LeaseGrant] = Field(default_factory=list)
    grant_requests: list[GrantRequest] = Field(default_factory=list)
    cancel_attempts: list[str] = Field(default_factory=list)
    poll_after_seconds: float = 2.0


class Heartbeat(_Model):
    attempt_id: str
    fencing_token: int = Field(ge=1)
    state: AttemptState
    provider_reference: str | None = None
    events: list[Event] = Field(default_factory=list, max_length=500)


class HeartbeatAck(_Model):
    lease_expires_at: datetime
    cancel_requested: bool
    accepted_events: int
    # A renewed attempt token; the agent uses it for the next report.
    attempt_token: str


class AttemptOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class AttemptResult(_Model):
    attempt_id: str
    fencing_token: int = Field(ge=1)
    outcome: AttemptOutcome
    failure_class: FailureClass | None = None
    exit_code: int | None = None
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    # Present on success: the attempt manifest as written by the runner, already
    # checked by the agent against the objects in the execution site's storage.
    attempt_manifest: dict[str, Any] | None = None
    attempt_manifest_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class ResultAck(_Model):
    job_state: str
    canonical: bool
    retry_scheduled: bool
