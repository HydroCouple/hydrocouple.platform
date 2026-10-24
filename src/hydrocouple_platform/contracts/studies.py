"""Studies: a registered evaluation function that callers drive with batches.

A study fixes what one evaluation means (a revision, an approved evaluator
workflow and its parameters, the decision dimensions, the objectives, a seed and
a failed-candidate policy) and how much may be spent (an evaluation budget and a
pool size). Callers submit batches of candidate vectors and read back their
costs, so any ask/tell optimizer can drive it.

Evaluations run on an evaluator pool: task jobs that keep the engine loaded and
lease chunks of a batch. A chunk lease is fenced by its lease number.
"""

from __future__ import annotations

import math
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class StudyState(str, Enum):
    PENDING_APPROVAL = "PENDING_APPROVAL"
    OPEN = "OPEN"  # accepts batches
    CLOSED = "CLOSED"  # accepts no more batches; finishing the queued ones
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


STUDY_TERMINAL = frozenset({StudyState.COMPLETED, StudyState.CANCELLED, StudyState.FAILED,
                            StudyState.REJECTED})


class BatchState(str, Enum):
    QUEUED = "QUEUED"
    DONE = "DONE"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class FailurePolicy(str, Enum):
    """What a failed candidate does, as in ``openswmm_gymnasium.optimize``.

    ``fail`` ends the study; ``penalty`` records the candidate with every
    objective set to the study's ``failure_penalty`` and continues.
    """

    FAIL = "fail"
    PENALTY = "penalty"


class Dimension(_Model):
    """One component of the decision vector."""

    label: str = Field(min_length=1, max_length=200)
    low: float
    high: float

    @model_validator(mode="after")
    def _bounds(self) -> Dimension:
        if not (math.isfinite(self.low) and math.isfinite(self.high)) or self.low > self.high:
            raise ValueError(f"dimension {self.label}: bounds must be finite with low <= high")
        return self


class Objective(_Model):
    name: str = Field(min_length=1, max_length=200)
    direction: Literal["minimize", "maximize"]


class StudyCreate(_Model):
    project_id: str
    session_id: str | None = None
    revision_id: str
    target_id: str
    workflow: str = "gym.evaluate"
    workflow_version: str = "1"
    parameters: dict[str, Any] = Field(default_factory=dict)
    dimensions: list[Dimension] = Field(min_length=1, max_length=10_000)
    objectives: list[Objective] = Field(min_length=1, max_length=32)
    seed: int | None = None
    on_failure: FailurePolicy = FailurePolicy.FAIL
    failure_penalty: float = 1e12
    max_evaluations: int = Field(ge=1, le=1_000_000)
    pool_size: int = Field(default=1, ge=1, le=256)
    profile: str | None = None
    tool_call_id: str | None = None

    @model_validator(mode="after")
    def _unique_names(self) -> StudyCreate:
        names = [o.name for o in self.objectives]
        if len(set(names)) != len(names):
            raise ValueError("objective names must be unique")
        if not math.isfinite(self.failure_penalty):
            raise ValueError("failure_penalty must be finite")
        return self


class BatchSubmit(_Model):
    candidates: list[list[float]] = Field(min_length=1, max_length=10_000)


class EvaluationRecord(_Model):
    """One candidate's outcome as the study reports it.

    ``costs`` are minimized (a ``maximize`` objective is negated by the
    evaluator), in the order of the study's objectives. A failed candidate under
    the ``penalty`` policy carries the penalty costs and ``failed=True``; under
    ``fail`` it carries no costs.
    """

    evaluation: int = Field(ge=1, description="1-based position in the study, in submission order")
    costs: list[float] | None
    failed: bool = False
    error: str | None = None
    steps: int = 0


class BatchView(_Model):
    batch_id: str
    study_id: str
    sequence: int
    state: BatchState
    size: int
    completed: int
    results: list[EvaluationRecord] | None = None
    next_action: str | None = None


class StudyView(_Model):
    study_id: str
    run_id: str
    project_id: str
    session_id: str | None
    revision_id: str
    target_id: str
    state: StudyState
    workflow: str
    workflow_version: str
    objectives: list[Objective]
    dimensions: int
    seed: int | None
    on_failure: FailurePolicy
    max_evaluations: int
    evaluations_submitted: int
    evaluations_completed: int
    evaluations_failed: int
    batches: int
    pool_size: int
    pool: list[dict[str, Any]] = Field(default_factory=list)
    approval_id: str | None = None
    failure: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime
    next_action: str | None = None


# -- agent protocol -----------------------------------------------------------------


class EvaluationResult(_Model):
    """One candidate as an evaluator reports it."""

    costs: list[float] | None = None
    steps: int = Field(default=0, ge=0)
    error: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _one_outcome(self) -> EvaluationResult:
        if (self.costs is None) == (self.error is None):
            raise ValueError("an evaluation result has costs or an error, not both or neither")
        if self.costs is not None and not all(math.isfinite(c) for c in self.costs):
            raise ValueError("costs must be finite")
        return self


class EvaluationLeaseRequest(_Model):
    attempt_id: str
    fencing_token: int = Field(ge=1)
    wait_seconds: float = Field(default=0, ge=0, le=30)


class EvaluationChunk(_Model):
    chunk_id: str
    lease_number: int = Field(ge=1)
    study_id: str
    batch_id: str
    start: int = Field(ge=0, description="index of the first candidate within its batch")
    first_evaluation: int = Field(ge=1)
    candidates: list[list[float]]
    lease_expires_at: datetime


class EvaluationLeaseResponse(_Model):
    chunk: EvaluationChunk | None = None
    # True when this task should finish: the study ended, or it has been idle too long.
    done: bool = False
    poll_after_seconds: float = 1.0


class ResultObject(_Model):
    key: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)


class EvaluationCompletion(_Model):
    attempt_id: str
    fencing_token: int = Field(ge=1)
    chunk_id: str
    lease_number: int = Field(ge=1)
    results: list[EvaluationResult] = Field(min_length=1)
    # The append-only evaluation records for this chunk, in the target's storage.
    object: ResultObject


class CompletionAck(_Model):
    accepted: bool
    # Why a completion was not accepted, e.g. the chunk was leased again.
    reason: str | None = None
