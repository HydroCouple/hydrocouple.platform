"""Schedules: the cycles of a digital twin, submitted by the control plane one after another.

A schedule fixes a revision, a resumable workflow and its parameters, and a cadence: cycle
``n`` ends at ``first_cycle_end + n * cycle_minutes`` (the model's UTC wall clock). The control
plane submits each cycle as a run that continues from the previous cycle's saved state, once
that cycle has succeeded (``replay``) or once its end has passed on the clock (``wall_clock``,
after ``lag_seconds`` for the telemetry to arrive). A cycle that does not succeed pauses the
schedule, so no cycle ever starts from a state other than its predecessor's; resuming the
schedule submits the failed cycle again.

Setpoint proposals come from a cycle's forecast (its control policy) or from an agent or a
person. Only a person decides one, and only an approved one is applied: the next cycle receives
the approved proposals that take effect before its end and applies each from its effective
time (or from the cycle's start, when that time has passed).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ScheduleState(str, Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"  # by a person, or because a cycle did not succeed
    COMPLETED = "COMPLETED"  # every cycle succeeded


class Pace(str, Enum):
    REPLAY = "replay"  # each cycle as soon as the one before it succeeded
    WALL_CLOCK = "wall_clock"  # each cycle once its end (UTC) and the lag have passed


class ScheduleCreate(_Model):
    project_id: str
    session_id: str | None = None
    revision_id: str
    target_id: str
    workflow: str
    workflow_version: str
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="The workflow's parameters for every cycle, without cycle_end, which the "
                    "schedule sets")
    first_cycle_end: datetime = Field(description="The first cycle's end, wall clock, no zone")
    cycle_minutes: float = Field(gt=0, le=10_080)
    cycles: int | None = Field(default=None, ge=1, le=100_000,
                               description="How many cycles; none for no end")
    pace: Pace = Pace.REPLAY
    lag_seconds: float = Field(default=0, ge=0, le=86_400)
    resume_from: str | None = Field(
        default=None, description="A published run the first cycle continues from")
    await_decisions: bool = Field(
        default=False, description="Submit a cycle only once every proposal is decided")
    profile: str | None = None
    retry_budget: int | None = Field(default=None, ge=0, le=10)
    estimated_runtime_seconds: float | None = Field(default=None, gt=0)
    tool_call_id: str | None = None


class ScheduleCycleView(_Model):
    number: int
    cycle_end: datetime
    attempt: int = Field(description="1 for the first submission, more after a resume")
    run_id: str
    job_id: str
    job_state: str
    resumed_from: str | None


class ScheduleView(_Model):
    schedule_id: str
    state: ScheduleState
    project_id: str
    revision_id: str
    target_id: str
    workflow: str
    workflow_version: str
    parameters: dict[str, Any]
    first_cycle_end: datetime
    cycle_minutes: float
    cycles: int | None
    pace: Pace
    lag_seconds: float
    await_decisions: bool
    next_cycle: int = Field(description="The cycle running now, or the next one to submit")
    last_run_id: str | None = Field(description="The last cycle that succeeded")
    pause_reason: dict[str, Any] | None
    agent_session_id: str | None
    history: list[ScheduleCycleView]
    created_at: datetime
    updated_at: datetime


class ProposalState(str, Enum):
    PROPOSED = "PROPOSED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    SUPERSEDED = "SUPERSEDED"  # undecided when a later cycle proposed again
    APPLIED = "APPLIED"


class ProposalCreate(_Model):
    """A setpoint an agent or a person proposes for a schedule's model."""

    link: str = Field(min_length=1)
    value: float = Field(ge=0, le=1)
    effective_from: datetime = Field(description="Wall clock, no zone")
    note: str | None = Field(default=None, max_length=2_000)
    tool_call_id: str | None = None


class ProposalDecision(_Model):
    approve: bool
    note: str | None = Field(default=None, max_length=2_000)


class ProposalView(_Model):
    proposal_id: str
    schedule_id: str
    state: ProposalState
    link: str
    property: str
    value: float
    effective_from: datetime
    issued_at: datetime | None = Field(description="The cycle end its forecast was issued at")
    run_id: str | None = Field(description="The cycle whose forecast proposed it")
    proposed_by: dict[str, Any]
    note: str | None
    decided_by: str | None = Field(description="The person who decided it")
    decided_at: datetime | None
    decision_note: str | None
    applied_run_id: str | None
    applied_at: datetime | None = Field(description="Model time it took effect")
    created_at: datetime
