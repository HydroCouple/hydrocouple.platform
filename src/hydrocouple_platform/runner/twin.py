"""``twin.cycle@1``: one cycle of a digital twin on the OpenSWMM engine.

A cycle continues the model from the state the previous cycle recorded, driven by the telemetry
that arrived since, to the cycle's end; records its own state for the next cycle; then forecasts
from that state over a horizon and proposes setpoints. It never applies a proposal: setpoints a
person approved arrive in ``setpoints`` and are applied at their effective times.

Telemetry is rain: each gage reads a series of the revision (a sidecar CSV, ISO times and one
column per series), cut before the cycle's end, as the engine's continuation needs (the
continued model may add series points after the record and change its end, nothing else). The
state is a ``[RESTART]`` continuation record in ``state.h5``; the next cycle's job names it as
an input (``resume``). Outputs: ``state.h5``, ``cycle.json`` (the end state and what the cycle
resumed from), ``forecast.json`` (forecast series and proposals) and the report.
"""

from __future__ import annotations

import csv
import time
from datetime import datetime, timedelta
from pathlib import Path

from ..contracts.manifests import ArtifactKind, canonical_json, check_safe_path
from . import EXIT_ENGINE, EXIT_INVALID_INPUT, EXIT_MISSING_DEPENDENCY, RunnerFailure
from .core import Context, HandlerResult
from .swmm import _component

RESUME_INPUT = "resume/state.h5"


def _series(path: Path, column: str | None, before: datetime) -> list[tuple[datetime, float]]:
    """(time, value) rows of one column of a wide CSV before ``before``; empty cells are left
    out, and rows from ``before`` on are not read (that telemetry has not arrived yet)."""
    with path.open(newline="", encoding="utf-8") as fh:
        rows = [r for r in csv.reader(fh) if r]
    header = [h.strip() for h in rows[0]]
    if column is None:
        if len(header) != 2:
            raise RunnerFailure(EXIT_INVALID_INPUT,
                                f"{path.name} has columns {header[1:]}; name one")
        index = 1
    elif column in header[1:]:
        index = header.index(column)
    else:
        raise RunnerFailure(EXIT_INVALID_INPUT, f"{path.name} has no column {column!r}")
    out = []
    for line, r in enumerate(rows[1:], start=2):
        if index < len(r) and r[index].strip():
            try:
                when = datetime.fromisoformat(r[0].strip())
                if when < before:
                    out.append((when, float(r[index])))
            except ValueError as exc:
                raise RunnerFailure(EXIT_INVALID_INPUT,
                                    f"{path.name} line {line}: {exc}") from None
    return out


def _feed(solver, model_dir: Path, telemetry: list[dict], cycle_end: datetime) -> dict[str, int]:
    """Point each gage at its telemetry series, cut before the cycle's end (after open)."""
    from openswmm.engine import GageRainType, catalog

    used = {}
    for t in telemetry:
        try:
            path = model_dir / check_safe_path(t["series"])
        except ValueError as exc:
            raise RunnerFailure(EXIT_INVALID_INPUT, f"telemetry of {t['gage']}: {exc}") from None
        if not path.is_file():
            raise RunnerFailure(EXIT_MISSING_DEPENDENCY, f"the telemetry {t['series']} is missing")
        rows = _series(path, t.get("column"), cycle_end)
        name = f"telemetry:{t['gage']}"
        table = catalog.resolve(solver, "tables", None).add_timeseries(name)
        for when, value in rows:
            table.add(when, value)
        gage = catalog.resolve(solver, "gage", t["gage"])
        gage.set_timeseries(name)
        gage.rain_type = GageRainType[t.get("format", "INTENSITY")]
        steps = [(b[0] - a[0]).total_seconds()
                 for a, b in zip(rows, rows[1:], strict=False) if b[0] > a[0]]
        if steps:
            gage.set_rain_interval(min(steps))
        used[t["gage"]] = len(rows)
    return used


def _open(model: Path, rpt: Path, end: datetime):
    from openswmm.engine import Solver, catalog

    solver = Solver(str(model), str(rpt), "")  # no binary output: the twin reports in JSON
    try:
        solver.open()
    except Exception as exc:
        messages = list(getattr(solver, "open_errors", []) or [])[:20]
        raise RunnerFailure(EXIT_INVALID_INPUT, "the engine could not open the model: "
                            + ("; ".join(messages) or str(exc))) from None
    catalog.resolve(solver, "options", None).end_datetime = end
    return solver


def _start(solver, what: str) -> None:
    """Initialize and start; the engine refuses a state it cannot continue from here, naming why
    (a model that changed in more than its end and the series points after the record)."""
    try:
        solver.initialize()
        solver.start()
    except Exception as exc:
        raise RunnerFailure(EXIT_INVALID_INPUT, f"the engine refused {what}: {exc}") from None


def _read(solver, path: str, element: str) -> float:
    from openswmm.engine import catalog

    kind, _, field = path.partition(".")
    return float(getattr(catalog.resolve(solver, kind, element), field))


def _curve(policy: dict, solver) -> float:
    """A control curve's setting: linear in the observed field between its knots."""
    x = _read(solver, policy["observe"], policy["element"])
    xs, ys = policy["x"], policy["setting"]
    if x <= xs[0]:
        return float(ys[0])
    for x0, y0, x1, y1 in zip(xs, ys, xs[1:], ys[1:], strict=False):
        if x <= x1:
            return float(y0 + (y1 - y0) * (x - x0) / (x1 - x0))
    return float(ys[-1])


def _cycle(ctx: Context, model: Path, p: dict, cycle_end: datetime, resume: Path | None) -> dict:
    from openswmm.engine import EngineState

    active = (EngineState.STARTED, EngineState.RUNNING)
    solver = _open(model, ctx.outputs / "model.rpt", cycle_end)
    try:
        fed = _feed(solver, model.parent, p.get("telemetry") or [], cycle_end)
        r = solver.restart
        if resume is not None:
            r.source_file, r.source_record, r.mode = str(resume), "LATEST", "CONTINUE"
        r.record_file, r.record_format = str(ctx.outputs / "state.h5"), "HDF5"
        r.record_at, r.record_continuation = ["END"], True
        pending = sorted(p.get("setpoints") or [], key=lambda s: s["effective_from"])
        _start(solver, "the cycle")
        started = solver.current_datetime
        steps, applied = 0, []
        while int(solver.state) in active:
            while pending and solver.current_datetime >= datetime.fromisoformat(
                    pending[0]["effective_from"]):
                s = pending.pop(0)
                try:
                    link = solver.links[s["link"]]
                except KeyError:
                    raise RunnerFailure(EXIT_INVALID_INPUT, f"setpoint {s['proposal_id']}: the "
                                        f"model has no link {s['link']!r}") from None
                link.target_setting = float(s["value"])
                applied.append(s | {"applied_at": solver.current_datetime.isoformat()})
            solver.step()
            steps += 1
            if steps % 200 == 0:
                ctx.check_cancel()
        state = {
            "nodes": {n.id: float(n.depth) for n in solver.nodes},
            "links": {lk.id: {"flow": float(lk.flow), "setting": float(lk.control_setting)}
                      for lk in solver.links},
        }
        solver.end()
        solver.report()
    except RunnerFailure:
        raise
    except Exception as exc:
        raise RunnerFailure(EXIT_ENGINE, f"the cycle failed: {exc}") from None
    finally:
        try:
            solver.close()
        except Exception:
            pass
        solver.destroy()
    return {"started_at": started.isoformat(), "ended_at": cycle_end.isoformat(),
            "routing_steps": steps, "telemetry_rows": fed, "setpoints_applied": applied,
            "end_state": state}


def _forecast(ctx: Context, model: Path, p: dict, cycle_end: datetime) -> dict:
    """Continue from the cycle's record over the horizon with no rain after the cycle's end."""
    from openswmm.engine import EngineState

    f = p["forecast"]
    horizon = timedelta(minutes=float(f["horizon_minutes"]))
    every = timedelta(minutes=float(f.get("every_minutes", 5)))
    active = (EngineState.STARTED, EngineState.RUNNING)
    from openswmm.engine import catalog

    solver = _open(model, ctx.outputs / "forecast.rpt", cycle_end + horizon)

    def unit(path: str) -> str | None:  # the model's unit of a catalog path, as the summary's
        try:
            kind = catalog.lookup(path).get("units")
        except KeyError:
            return None
        return catalog.unit_label(kind, solver.unit_system, solver.flow_units)

    series = [{"path": o["path"], "element_id": o["id"], "unit": unit(o["path"]), "times": [],
               "values": []} for o in f.get("observe") or []]
    policy, proposals, last = p.get("policy"), [], None

    def decide(at: datetime) -> None:
        """The policy's setting from ``at`` on, rounded to its resolution; a change is a
        proposal effective at ``at`` (the issue time or a sample time, whole seconds)."""
        nonlocal last
        resolution = float(policy.get("resolution", 0.05))
        setting = round(round(_curve(policy, solver) / resolution) * resolution, 6)
        if setting != last:
            proposals.append({"link": policy["link"], "property": "link.target_setting",
                              "value": setting, "effective_from": at.isoformat()})
            last = setting
        solver.links[policy["link"]].target_setting = setting

    try:
        _feed(solver, model.parent, p.get("telemetry") or [], cycle_end)
        r = solver.restart
        r.source_file, r.source_record, r.mode = str(ctx.outputs / "state.h5"), "LATEST", "CONTINUE"
        _start(solver, "the forecast")
        if policy is not None:
            decide(cycle_end)
        due, end = cycle_end + every, cycle_end + horizon
        while int(solver.state) in active:
            solver.step()
            # One sample per sample time up to the horizon: the step that ends the run does not
            # advance the clock, so it is no sample of its own.
            if due <= end and solver.current_datetime >= due - timedelta(milliseconds=1):
                for s in series:
                    s["times"].append(due.isoformat())
                    s["values"].append(_read(solver, s["path"], s["element_id"]))
                if policy is not None and due < end:
                    decide(due)
                due += every
        solver.end()
    except RunnerFailure:
        raise
    except Exception as exc:
        raise RunnerFailure(EXIT_ENGINE, f"the forecast failed: {exc}") from None
    finally:
        try:
            solver.close()
        except Exception:
            pass
        solver.destroy()
    return {"issued_at": cycle_end.isoformat(), "horizon_minutes": float(f["horizon_minutes"]),
            "rain_after_issue": "none", "series": series, "proposals": proposals}


def run(ctx: Context, composition: dict) -> HandlerResult:
    comp = _component(composition)
    model = ctx.inputs / comp["arguments"]["configuration"]["model"]
    if not model.is_file():
        raise RunnerFailure(EXIT_MISSING_DEPENDENCY, f"the model {model.name} is missing")
    p = ctx.spec.workload.parameters
    cycle_end = datetime.fromisoformat(p["cycle_end"])
    policy = p.get("policy")
    if policy is not None and (len(policy["x"]) != len(policy["setting"]) or any(
            b <= a for a, b in zip(policy["x"], policy["x"][1:], strict=False))):
        raise RunnerFailure(EXIT_INVALID_INPUT, "the policy needs as many settings as knots, "
                            "and knots that increase")
    resume = ctx.inputs / RESUME_INPUT
    resume = resume if resume.is_file() else None
    ctx.outputs.mkdir(parents=True, exist_ok=True)
    import openswmm

    began = time.monotonic()
    ctx.events.emit("lifecycle", "running", f"cycle to {cycle_end.isoformat()}"
                    + (", resumed" if resume else ", first cycle"))
    cycle = _cycle(ctx, model, p, cycle_end, resume)
    resumed = next((a for a in ctx.spec.inputs.artifacts if a.path == RESUME_INPUT), None)
    cycle["resumed_from"] = resumed.model_dump(mode="json") if resumed else None
    (ctx.outputs / "cycle.json").write_bytes(canonical_json(cycle))
    artifacts = [("model.rpt", ArtifactKind.REPORT, "swmm_rpt"),
                 ("state.h5", ArtifactKind.STATE, "openswmm_restart_hdf5"),
                 ("cycle.json", ArtifactKind.SUMMARY, "json")]
    quality = {"routing_steps": cycle["routing_steps"], "resumed": resume is not None,
               "setpoints_applied": cycle["setpoints_applied"]}
    if p.get("forecast"):
        forecast = _forecast(ctx, model, p, cycle_end)
        (ctx.outputs / "forecast.json").write_bytes(canonical_json(forecast))
        artifacts += [("forecast.json", ArtifactKind.FORECAST, "json"),
                      ("forecast.rpt", ArtifactKind.REPORT, "swmm_rpt")]
        # Proposals travel in the attempt manifest, which the control plane always receives.
        quality["proposals"] = forecast["proposals"]
    quality["seconds"] = round(time.monotonic() - began, 3)
    return HandlerResult(
        status="completed",
        components=[{"id": comp["id"], "final_status": "Finished", "errors": []}],
        quality=quality,
        engine_version=getattr(openswmm, "__version__", None),
        artifacts=artifacts,
    )
