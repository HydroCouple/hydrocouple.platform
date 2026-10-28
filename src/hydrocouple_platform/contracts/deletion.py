"""Deleting a run (or a study, through its run): asynchronous, confirmed erasure (plan §12).

1. The run is marked deleting. It takes no new work and no new grants, and its active
   attempts are asked to stop.
2. Erasure waits until no attempt of the run can still write. If that cannot be confirmed
   in time (an unreachable agent, an attempt that does not stop), the deletion is
   ``BLOCKED`` and says why. It continues when the writers are gone.
3. The target's agent deletes the run's objects under its prefix, object versions
   included where the store keeps them, and reports how many it deleted.
4. A second pass after a delay removes anything written late. The deletion is ``ERASED``
   only when a pass finds nothing left.

Shared inputs (the revision) are kept. The run's record stays as a tombstone, and its
artifact records are removed.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DeletionState(str, Enum):
    REQUESTED = "REQUESTED"
    WAITING_FOR_WRITERS = "WAITING_FOR_WRITERS"
    BLOCKED = "BLOCKED"  # erasure blocked: writers not confirmed gone; retried
    ERASING = "ERASING"
    VERIFYING = "VERIFYING"  # erased once; a later pass checks for late writes
    ERASED = "ERASED"


class DeletionView(_Model):
    deletion_id: str
    run_id: str
    study_id: str | None = None
    state: DeletionState
    prefixes: list[str]
    passes: int
    objects_deleted: int
    reason: str | None = None
    requested_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    next_action: str | None = None


# -- agent protocol ---------------------------------------------------------------------


class Erasure(_Model):
    """Delete every object under ``prefixes`` and report how many there were."""

    deletion_id: str
    pass_number: int = Field(ge=1)
    prefixes: list[str] = Field(min_length=1)


class ErasureReport(_Model):
    deletion_id: str
    pass_number: int = Field(ge=1)
    ok: bool
    objects_deleted: int = Field(default=0, ge=0)
    problems: list[str] = Field(default_factory=list)
