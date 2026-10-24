"""Scheduler adapters: one contract for every scheduler the agent can drive.

``capabilities / validate / submit / inspect / cancel / cleanup``. The local
pool is implemented here; Kubernetes (M4) and the managed Batch, serverless
container job and SLURM adapters (M5) implement the same protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ...contracts.agent import Event, LeaseGrant
from ...contracts.capabilities import AdapterCapabilities, check_spec
from ...contracts.jobs import JobSpec
from ...contracts.states import AttemptState


@dataclass
class Observation:
    """What an adapter can say about one submitted attempt."""

    state: AttemptState
    finished: bool = False
    exit_code: int | None = None
    signal: int | None = None
    cancelled_by_agent: bool = False
    timed_out: bool = False
    provider_reference: str | None = None
    events: list[Event] = field(default_factory=list)
    stderr_tail: str | None = None


class SchedulerAdapter(Protocol):
    name: str

    def capabilities(self) -> AdapterCapabilities: ...
    def free_slots(self) -> int: ...
    # ``env`` carries extra environment for the runner (the gateway for evaluator tasks).
    def submit(self, lease: LeaseGrant, store_uri: str,
               env: dict[str, str] | None = None) -> str: ...
    def inspect(self, handle: str) -> Observation: ...
    def cancel(self, handle: str) -> None: ...
    def cleanup(self, handle: str) -> None: ...


def validate(adapter: SchedulerAdapter, spec: JobSpec) -> list[str]:
    return check_spec(adapter.capabilities(), spec)


__all__ = ["Observation", "SchedulerAdapter", "validate"]
