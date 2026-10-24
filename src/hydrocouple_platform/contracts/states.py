"""Job and attempt lifecycle states and the transitions allowed between them.

Validation is synchronous inside a submission request: an invalid request
returns a typed error and creates nothing, so ``VALIDATING`` is never stored.

A job's terminal states never change again. An attempt's terminal states never
return to running; a retry creates a new attempt with the next fencing token.
"""

from __future__ import annotations

from enum import Enum


class JobState(str, Enum):
    PENDING_APPROVAL = "PENDING_APPROVAL"
    ACCEPTED = "ACCEPTED"
    LEASABLE = "LEASABLE"
    LEASED = "LEASED"
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FINALIZING = "FINALIZING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    REJECTED = "REJECTED"


class AttemptState(str, Enum):
    LEASED = "LEASED"
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FINALIZING = "FINALIZING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    LOST = "LOST"


JOB_TERMINAL = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED, JobState.TIMED_OUT, JobState.REJECTED}
)
ATTEMPT_TERMINAL = frozenset(
    {
        AttemptState.SUCCEEDED,
        AttemptState.FAILED,
        AttemptState.CANCELLED,
        AttemptState.TIMED_OUT,
        AttemptState.LOST,
    }
)
# States in which an attempt holds the job's lease and may report.
JOB_HELD = frozenset(
    {JobState.LEASED, JobState.QUEUED, JobState.STARTING, JobState.RUNNING, JobState.FINALIZING}
)
# States an agent may report as progress of a held attempt, in order.
PROGRESS_ORDER = (
    AttemptState.LEASED,
    AttemptState.QUEUED,
    AttemptState.STARTING,
    AttemptState.RUNNING,
    AttemptState.FINALIZING,
)

_J = JobState
_HELD_EXITS = {_J.LEASABLE, _J.FAILED, _J.CANCELLED, _J.TIMED_OUT}

JOB_TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    _J.PENDING_APPROVAL: frozenset({_J.ACCEPTED, _J.REJECTED, _J.CANCELLED}),
    _J.ACCEPTED: frozenset({_J.LEASABLE, _J.CANCELLED}),
    _J.LEASABLE: frozenset({_J.LEASED, _J.CANCELLED, _J.TIMED_OUT}),
    _J.LEASED: frozenset({_J.QUEUED, _J.STARTING, _J.RUNNING, _J.FINALIZING} | _HELD_EXITS),
    _J.QUEUED: frozenset({_J.STARTING, _J.RUNNING, _J.FINALIZING} | _HELD_EXITS),
    _J.STARTING: frozenset({_J.RUNNING, _J.FINALIZING} | _HELD_EXITS),
    _J.RUNNING: frozenset({_J.FINALIZING, _J.SUCCEEDED} | _HELD_EXITS),
    _J.FINALIZING: frozenset({_J.SUCCEEDED} | _HELD_EXITS),
    _J.SUCCEEDED: frozenset(),
    _J.FAILED: frozenset(),
    _J.CANCELLED: frozenset(),
    _J.TIMED_OUT: frozenset(),
    _J.REJECTED: frozenset(),
}

_A = AttemptState
_ATTEMPT_EXITS = {_A.FAILED, _A.CANCELLED, _A.TIMED_OUT, _A.LOST}

ATTEMPT_TRANSITIONS: dict[AttemptState, frozenset[AttemptState]] = {
    _A.LEASED: frozenset({_A.QUEUED, _A.STARTING, _A.RUNNING, _A.FINALIZING} | _ATTEMPT_EXITS),
    _A.QUEUED: frozenset({_A.STARTING, _A.RUNNING, _A.FINALIZING} | _ATTEMPT_EXITS),
    _A.STARTING: frozenset({_A.RUNNING, _A.FINALIZING} | _ATTEMPT_EXITS),
    _A.RUNNING: frozenset({_A.FINALIZING, _A.SUCCEEDED} | _ATTEMPT_EXITS),
    _A.FINALIZING: frozenset({_A.SUCCEEDED} | _ATTEMPT_EXITS),
    _A.SUCCEEDED: frozenset(),
    _A.FAILED: frozenset(),
    _A.CANCELLED: frozenset(),
    _A.TIMED_OUT: frozenset(),
    _A.LOST: frozenset(),
}


def job_can(current: JobState, target: JobState) -> bool:
    """True when a job may move from ``current`` to ``target``."""
    return target in JOB_TRANSITIONS[current]


def attempt_can(current: AttemptState, target: AttemptState) -> bool:
    """True when an attempt may move from ``current`` to ``target``."""
    return target in ATTEMPT_TRANSITIONS[current]


def is_forward_progress(current: AttemptState, reported: AttemptState) -> bool:
    """True when ``reported`` is a later progress state than ``current``.

    Late or out-of-order heartbeats never move an attempt backwards.
    """
    if current not in PROGRESS_ORDER or reported not in PROGRESS_ORDER:
        return False
    return PROGRESS_ORDER.index(reported) > PROGRESS_ORDER.index(current)
