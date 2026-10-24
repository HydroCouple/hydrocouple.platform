"""``swmm.simulation@1``: run one SWMM model to completion with the OpenSWMM engine.

The tight step loop stays inside this process. Progress is reported on wall
time intervals as simulated time completed; no percentage or ETA is invented.
"""

from __future__ import annotations

import time
from pathlib import Path

from ..contracts.manifests import ArtifactKind, check_safe_path
from ..workflows import OPENSWMM_ENGINE_COMPONENT
from . import EXIT_ENGINE, EXIT_INVALID_INPUT, EXIT_MISSING_DEPENDENCY, RunnerFailure
from .core import Context, HandlerResult


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
        solver.end()
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
