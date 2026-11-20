"""The run summary: columns bound to catalog paths and units, and agreement with the report."""

from __future__ import annotations

import json
import math
import subprocess
import sys
from dataclasses import dataclass
from importlib import resources

import jsonschema
import pytest

from conftest import DATA
from hydrocouple_platform.contracts.manifests import canonical_json
from hydrocouple_platform.runner.summary import CATALOG_PATHS, build_summary


@dataclass
class _Continuity:
    continuity_error_pct: float
    total_flooding: float


@dataclass
class _Node:
    node_id: str
    node_type: str
    max_depth: float
    max_overflow_rate: float
    total_flood_volume: float
    time_flooded: float


@dataclass
class _Snapshot:
    routing_continuity: _Continuity
    node_flooding: list


UNITS = {"node.stats.max_depth": "ft", "node.stats.max_overflow": "CFS",
         "node.stats.vol_flooded": "ft3"}


def _schema() -> dict:
    files = resources.files("hydrocouple_platform") / "schemas" / "platform"
    return json.loads((files / "run-summary-1.json").read_text(encoding="utf-8"))


def test_columns_carry_the_catalog_path_and_the_engines_unit():
    snapshot = _Snapshot(_Continuity(0.02, 31.5),
                         [_Node("J10", "JUNCTION", 4.2, 12.5, 231937.0, math.inf)])
    summary = build_summary(snapshot, UNITS.get, engine="6.0.0a4.dev1", unit_system="US",
                            flow_units="CFS")
    table = summary.tables["node_flooding"]
    assert table.columns["total_flood_volume"].catalog_path == "node.stats.vol_flooded"
    assert table.columns["total_flood_volume"].unit == "ft3"
    # The engine declares no unit here, so none is invented.
    assert table.columns["time_flooded"].unit is None
    assert table.rows == [{"node_id": "J10", "node_type": "JUNCTION", "max_depth": 4.2,
                           "max_overflow_rate": 12.5, "total_flood_volume": 231937.0,
                           "time_flooded": None}]  # not finite: left out, never NaN in JSON
    assert summary.tables["routing_continuity"].rows == [
        {"continuity_error_pct": 0.02, "total_flooding": 31.5}]
    assert summary.tables["routing_continuity"].columns == {}
    document = json.loads(canonical_json(summary.model_dump(mode="json", by_alias=True)))
    jsonschema.validate(document, _schema())
    assert document["schema"] == "hydrocouple.platform/run-summary/1"


def report_table(text: str, title: str) -> list[list[str]]:
    """The data rows of one table of a SWMM text report, split into columns."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == title)
    rows: list[list[str]] = []
    rules = 0
    for line in lines[start + 2:]:
        stripped = line.strip()
        if stripped.startswith("***"):
            break  # the next table: this one had no rows
        if stripped.startswith("---"):
            rules += 1
            if rules == 3:
                break
        elif rules == 2 and stripped:
            rows.append(stripped.split())
    return rows


def test_report_table_reads_rows_and_an_empty_table():
    text = "\n".join([
        "  Node Flooding Summary", "  *********************", "", "  No nodes were flooded.", "",
        "  ********************", "  Link Flow Summary", "  ********************", "",
        "  ----------", "  Link  Type  CFS", "  ----------",
        "  C1    CONDUIT   4.17  0  01:05  2.31  0.26  0.35",
        "  P1    PUMP      1.00  0  00:10",
        "  ----------", "  Other"])
    assert report_table(text, "Node Flooding Summary") == []
    assert [r[:3] for r in report_table(text, "Link Flow Summary")] == [
        ["C1", "CONDUIT", "4.17"], ["P1", "PUMP", "1.00"]]


@pytest.mark.engine
def test_the_summary_agrees_with_the_text_report(artifact_dir):
    """On the real engine: peak link flows, and flooding where there is any, match model.rpt."""
    pytest.importorskip("openswmm.engine")
    model = DATA / "site_drainage_example.inp"
    probe = subprocess.run(
        [sys.executable, "-c",
         "import sys; from openswmm.engine import Solver; "
         "s = Solver(sys.argv[1], '/dev/null', '/dev/null'); s.open(); s.close(); s.destroy()",
         str(model)], capture_output=True, timeout=120)
    if probe.returncode != 0:
        pytest.skip(f"the installed openswmm engine cannot run here (exit {probe.returncode})")
    script = (
        "import sys, openswmm\n"
        "from openswmm.engine import EngineState, Solver\n"
        "from hydrocouple_platform.contracts.manifests import canonical_json\n"
        "from hydrocouple_platform.runner.summary import engine_summary\n"
        "inp, out = sys.argv[1], sys.argv[2]\n"
        "s = Solver(inp, out + '/model.rpt', out + '/model.out')\n"
        "s.open(); s.initialize(); s.start()\n"
        "while int(s.state) in (EngineState.STARTED, EngineState.RUNNING): s.step()\n"
        "s.end()\n"
        "summary = engine_summary(s, openswmm.__version__)\n"
        "s.report(); s.close(); s.destroy()\n"
        "open(out + '/summary.json', 'wb').write("
        "canonical_json(summary.model_dump(mode='json', by_alias=True)))\n")
    run = subprocess.run([sys.executable, "-c", script, str(model), str(artifact_dir)],
                         capture_output=True, text=True, timeout=300)
    assert run.returncode == 0, run.stderr

    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    jsonschema.validate(summary, _schema())
    report = (artifact_dir / "model.rpt").read_text(encoding="utf-8")
    tables = summary["tables"]
    for section, columns in CATALOG_PATHS.items():
        for column in columns:
            assert tables[section]["columns"][column]["unit"], f"{section}.{column} has no unit"

    links = {row["link_id"]: row for row in tables["link_flow_summary"]["rows"]}
    reported = report_table(report, "Link Flow Summary")
    assert {r[0] for r in reported} == set(links) and links
    assert tables["link_flow_summary"]["columns"]["max_flow"]["unit"] == summary["flow_units"]
    for name, _type, flow, *_ in reported:
        assert abs(links[name]["max_flow"]) == pytest.approx(float(flow), abs=0.006), name

    flooded = {row["node_id"]: row for row in tables["node_flooding"]["rows"]}
    reported = report_table(report, "Node Flooding Summary")
    assert {r[0] for r in reported} == set(flooded)
    seconds = {"s": 1.0, "h": 3600.0}[tables["node_flooding"]["columns"]["time_flooded"]["unit"]]
    for name, hours, rate, *_ in reported:
        flooded_hours = flooded[name]["time_flooded"] * seconds / 3600
        assert flooded_hours == pytest.approx(float(hours), abs=0.006), name
        assert flooded[name]["max_overflow_rate"] == pytest.approx(float(rate), abs=0.006), name
