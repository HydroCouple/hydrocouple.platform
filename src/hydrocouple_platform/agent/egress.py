"""Egress policy: what may leave the execution site.

Applied by the agent to every event and diagnostic before it is sent to the
control plane.

- ``metrics-only``: lifecycle phases and numeric progress; no text at all.
- ``summaries``: adds warnings and short messages (element IDs may appear).
- ``full``: adds logs and full diagnostics.
"""

from __future__ import annotations

from typing import Any

from ..contracts.agent import Event, EventKind
from ..contracts.jobs import EgressPolicy

SUMMARY_LIMIT = 500


def filter_event(event: Event, policy: EgressPolicy) -> Event | None:
    if policy == EgressPolicy.FULL:
        return event
    if policy == EgressPolicy.METRICS_ONLY:
        if event.kind not in (EventKind.LIFECYCLE, EventKind.PROGRESS):
            return None
        return event.model_copy(update={"message": None})
    if event.kind == EventKind.LOG:
        return None
    message = event.message[:SUMMARY_LIMIT] if event.message else None
    return event.model_copy(update={"message": message})


def filter_diagnostics(data: dict[str, Any], policy: EgressPolicy) -> dict[str, Any]:
    if policy == EgressPolicy.FULL:
        return data
    keep_numbers = {k: v for k, v in data.items() if isinstance(v, (int, float, bool))}
    if policy == EgressPolicy.METRICS_ONLY:
        return keep_numbers
    message = data.get("message")
    if isinstance(message, str):
        keep_numbers["message"] = message[:SUMMARY_LIMIT]
    return keep_numbers
