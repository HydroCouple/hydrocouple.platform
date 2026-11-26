"""``swmm.simulation@1`` and ``@2``: run one SWMM model to completion with the OpenSWMM engine.

The tight step loop stays inside this process. Progress is reported on wall
time intervals as simulated time completed; no percentage or ETA is invented.
Version 2 writes the request's overrides after the model opens and before it
initializes; the attempt's quality lists each with the value it replaced.
"""

from __future__ import annotations

import time
from pathlib import Path

from ..contracts.manifests import ArtifactKind, canonical_json, check_safe_path
from ..workflows import OPENSWMM_ENGINE_COMPONENT
from . import EXIT_ENGINE, EXIT_INVALID_INPUT, EXIT_MISSING_DEPENDENCY, RunnerFailure
from .core import Context, HandlerResult
from .summary import engine_summary


def _component(composition: dict) -> dict:
    components = composition.get("components", [])
    if len(components) != 1:
        raise RunnerFailure(EXIT_INVALID_INPUT,
                            "swmm.simulation runs exactly one component; use a composition "
                            "workflow for coupled models")
    comp = components[0]
    if comp.get("info", {}).get("component_info_id") != OPENSWMM_ENGINE_COMPONENT:
        raise RunnerFailure(EXIT_INVALID_INPUT,
                            f"the component must be {OPENSWMM_ENGINE_COMPONENT}")
    config = comp.get("arguments", {}).get("configuration", {})
    if config.get("schema") != "openswmm.component/1" or not config.get("model"):
        raise RunnerFailure(EXIT_INVALID_INPUT,
                            "the component configuration must use openswmm.component/1 and "
                            "name a model")
    try:
        check_safe_path(config["model"])
    except ValueError as exc:
        raise RunnerFailure(EXIT_INVALID_INPUT, str(exc)) from None
    return comp


_SCALAR = {"float", "int", "bool"}


def apply_overrides(solver, overrides: list[dict]) -> list[dict]:
    """Write each ``{path, id, value}`` to the opened model; return them with their base values
    and their unit labels in the model's unit system.

    A path must be a writable ``float``, ``int`` or ``bool`` field of an element kind in the
    engine catalog. Anything else, an unknown element or a value of the wrong type is invalid
    input, and nothing after it is written.
    """
    from openswmm.engine import catalog

    applied = []
    for number, item in enumerate(overrides, start=1):
        path, element, value = item["path"], item["id"], item["value"]
        where = f"override {number} ({path} on {element})"
        try:
            entry = catalog.lookup(path)
        except KeyError:
            raise RunnerFailure(EXIT_INVALID_INPUT, f"{where}: not an engine field") from None
        kind = path.split(".", 1)[0]
        if (entry["form"] != "property" or entry.get("access") != "rw"
                or entry.get("type") not in _SCALAR
                or "collection" not in catalog.targets().get(kind, {})):
            raise RunnerFailure(EXIT_INVALID_INPUT,
                                f"{where}: only writable float, int or bool element fields "
                                "can be overridden")
        kind_ok = {"bool": isinstance(value, bool),
                   "int": isinstance(value, int) and not isinstance(value, bool),
                   "float": isinstance(value, (int, float)) and not isinstance(value, bool)}
        if not kind_ok[entry["type"]]:
            raise RunnerFailure(EXIT_INVALID_INPUT, f"{where}: needs a {entry['type']} value")
        try:
            owner = catalog.resolve(solver, entry["target"], element)
            base = getattr(owner, entry["name"])
            setattr(owner, entry["name"], float(value) if entry["type"] == "float" else value)
        except Exception as exc:
            raise RunnerFailure(EXIT_INVALID_INPUT, f"{where}: {exc}") from None
        unit = catalog.unit_label(entry.get("units"), solver.unit_system, solver.flow_units)
        applied.append({"path": path, "id": element, "value": value, "base_value": base,
                        "unit": unit})
    return applied


def run(ctx: Context, composition: dict) -> HandlerResult:
    comp = _component(composition)
    model = ctx.inputs / comp["arguments"]["configuration"]["model"]
    if not model.is_file():
        raise RunnerFailure(EXIT_MISSING_DEPENDENCY, f"the model {model.name} is missing")
    before = {p for p in model.parent.rglob("*") if p.is_file()}
    ctx.outputs.mkdir(parents=True, exist_ok=True)
    rpt, out = ctx.outputs / "model.rpt", ctx.outputs / "model.out"
    interval = float(ctx.spec.workload.parameters.get("progress_interval_seconds", 2.0))

    import openswmm
    from openswmm.engine import EngineState, Solver

    active = (EngineState.STARTED, EngineState.RUNNING)
    solver = Solver(str(model), str(rpt), str(out))
    errors: list[str] = []
    summary = None
    try:
        try:
            solver.open()
        except Exception as exc:
            messages = list(getattr(solver, "open_errors", []) or [])[:20]
            raise RunnerFailure(EXIT_INVALID_INPUT,
                                "the engine could not open the model: "
                                + ("; ".join(messages) or str(exc))) from None
        warnings = list(getattr(solver, "open_warnings", []) or [])[:50]
        for message in warnings:
            ctx.events.emit("warning", "open", message)
        overrides = apply_overrides(solver, ctx.spec.workload.parameters.get("overrides") or [])
        threads = solver.effective_threads(ctx.spec.runtime.threads)._asdict()
        solver.initialize()
        solver.start()
        start, end = solver.start_datetime, solver.end_datetime
        total = max((end - start).total_seconds(), 1.0)
        ctx.events.emit("progress", "running", completed=0.0, total=total,
                        unit="simulated_seconds")
        steps, last = 0, time.monotonic()
        while int(solver.state) in active:
            solver.step()
            steps += 1
            if steps % 200 == 0:
                ctx.check_cancel()
                now = time.monotonic()
                if now - last >= interval:
                    done = (solver.current_datetime - start).total_seconds()
                    ctx.events.emit("progress", "running", completed=min(done, total),
                                    total=total, unit="simulated_seconds")
                    last = now
        ctx.check_cancel()
        ctx.events.emit("progress", "running", completed=total, total=total,
                        unit="simulated_seconds")
        mb = solver.mass_balance
        quality = {
            "continuity_errors": {
                "runoff": float(mb.runoff_continuity_error),
                "routing": float(mb.routing_continuity_error),
            },
            "continuity_note": "fractions: 0.001 means 0.1 %",
            "routing_steps": steps,
            "warnings": len(warnings),
            "threads": threads,
        }
        if overrides:
            quality["overrides"] = overrides
        solver.end()
        try:
            summary = engine_summary(solver, getattr(openswmm, "__version__", None))
        except Exception as exc:  # the run itself is good; say why it has no summary
            ctx.events.emit("warning", "summary", f"no run summary: {exc}")
        solver.report()
    except RunnerFailure:
        raise
    except Exception as exc:
        errors.append(str(exc))
        raise RunnerFailure(EXIT_ENGINE, f"the engine failed: {exc}") from None
    finally:
        try:
            solver.close()
        except Exception:
            pass
        solver.destroy()

    artifacts = [("model.rpt", ArtifactKind.REPORT, "swmm_rpt"),
                 ("model.out", ArtifactKind.BINARY_OUTPUT, "swmm_out")]
    if summary is not None:
        (ctx.outputs / "summary.json").write_bytes(
            canonical_json(summary.model_dump(mode="json", by_alias=True)))
        artifacts.append(("summary.json", ArtifactKind.SUMMARY, "json"))
    # Files the model saved next to itself (e.g. [FILES] SAVE HOTSTART) are outputs too.
    for path in sorted(p for p in model.parent.rglob("*") if p.is_file() and p not in before):
        rel = "saved/" + path.relative_to(model.parent).as_posix()
        dest = ctx.outputs / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        path.replace(dest)
        artifacts.append((rel, ArtifactKind.OTHER, Path(rel).suffix.lstrip(".") or "binary"))
    return HandlerResult(
        status="completed",
        components=[{"id": comp["id"], "final_status": "Finished", "errors": errors}],
        quality=quality,
        engine_version=getattr(openswmm, "__version__", None),
        artifacts=artifacts,
    )
