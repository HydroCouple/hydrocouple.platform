"""The run summary: the engine's end-of-run report as data.

``openswmm.engine.get_report_snapshot`` returns the tables of the text report as
dataclasses. This turns them into a :class:`RunSummary` and says, for each
column the engine catalog describes, which catalog path it reports and in what
unit. Units come from the engine, never from here.
"""

from __future__ import annotations

import dataclasses
import math
import numbers
from collections.abc import Callable
from typing import Any

from ..contracts.manifests import RunSummary, SummaryColumn, SummaryTable

_NODE = {"max_depth": "node.stats.max_depth", "max_overflow_rate": "node.stats.max_overflow",
         "total_flood_volume": "node.stats.vol_flooded", "time_flooded": "node.stats.time_flooded"}
# Section of the report snapshot -> column -> the catalog path of the statistic the engine
# reads for it (see get_report_snapshot in openswmm/engine/_report.py).
CATALOG_PATHS: dict[str, dict[str, str]] = {
    "node_flooding": _NODE,
    "storage_summary": _NODE,
    "link_flow_summary": {
        "max_flow": "link.stats.max_flow", "max_velocity": "link.stats.max_velocity",
        "max_filling": "link.stats.max_filling", "total_volume": "link.stats.vol_flow",
        "surcharge_time": "link.stats.surcharge_time"},
    "pump_summary": {
        "num_startups": "link.stats.pump_cycles", "total_on_time": "link.stats.pump_on_time",
        "total_volume": "link.stats.pump_volume"},
    "subcatchment_summary": {
        "total_precip": "subcatchment.stats.precip",
        "total_runoff_vol": "subcatchment.stats.runoff_vol",
        "max_runoff_rate": "subcatchment.stats.max_runoff"},
}


def _plain(value: Any) -> float | int | str | None:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    number = float(value)
    return number if math.isfinite(number) else None


def build_summary(snapshot: Any, unit_of: Callable[[str], str | None], *,
                  engine: str | None = None, unit_system: str | None = None,
                  flow_units: str | None = None) -> RunSummary:
    """A :class:`RunSummary` from a report snapshot.

    ``unit_of`` gives the unit label of a catalog path for this model, or None
    when the engine declares none.
    """
    tables = {}
    for section in dataclasses.fields(snapshot):
        value = getattr(snapshot, section.name)
        entries = value if isinstance(value, list) else [value]
        tables[section.name] = SummaryTable(
            columns={column: SummaryColumn(catalog_path=path, unit=unit_of(path))
                     for column, path in CATALOG_PATHS.get(section.name, {}).items()},
            rows=[{k: _plain(v) for k, v in dataclasses.asdict(e).items()} for e in entries],
        )
    return RunSummary(engine=engine, unit_system=unit_system, flow_units=flow_units,
                      tables=tables)


def engine_summary(solver: Any, engine: str | None) -> RunSummary:
    """The summary of a run that has ended, with units from the engine's own catalog."""
    from openswmm.engine import catalog, get_report_snapshot

    unit_system, flow_units = solver.unit_system, solver.flow_units

    def unit_of(path: str) -> str | None:
        try:
            kind = catalog.lookup(path).get("units")
        except KeyError:
            return None
        return catalog.unit_label(kind, unit_system, flow_units)

    return build_summary(get_report_snapshot(solver), unit_of, engine=engine,
                         unit_system=unit_system,
                         flow_units=str(getattr(flow_units, "name", flow_units)))
