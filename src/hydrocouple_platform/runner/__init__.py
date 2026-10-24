"""Approved workflow runners.

A runner is started by an agent's scheduler adapter with a job spec, an attempt
identity and a storage location. It stages and verifies inputs, runs one
handler next to the engine, uploads outputs under the attempt's own prefix, and
writes the run manifest and attempt manifest. It reports to its agent only:
events are JSON lines on stdout, and the exit code classifies the outcome.
"""

from __future__ import annotations

from ..contracts.jobs import FailureClass

EVENT_PREFIX = "HCP-EVENT "

EXIT_OK = 0
EXIT_UNKNOWN = 1
EXIT_INVALID_INPUT = 10
EXIT_MISSING_DEPENDENCY = 11
EXIT_ENGINE = 12
EXIT_TRANSFER = 13
EXIT_CANCELLED = 14
EXIT_TIMEOUT = 15

EXIT_CLASS: dict[int, FailureClass] = {
    EXIT_INVALID_INPUT: FailureClass.INVALID_INPUT,
    EXIT_MISSING_DEPENDENCY: FailureClass.MISSING_DEPENDENCY,
    EXIT_ENGINE: FailureClass.ENGINE_CRASH,
    EXIT_TRANSFER: FailureClass.TRANSFER,
    EXIT_TIMEOUT: FailureClass.TIMEOUT,
}


class RunnerFailure(Exception):
    """A classified failure; the runner exits with ``exit_code``."""

    def __init__(self, exit_code: int, message: str):
        super().__init__(message)
        self.exit_code = exit_code
