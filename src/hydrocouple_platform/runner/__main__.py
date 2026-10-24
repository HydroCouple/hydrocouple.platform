"""Runner entry point: ``python -m hydrocouple_platform.runner --spec ... --attempt-id ...``."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import traceback
from pathlib import Path

from ..contracts.jobs import JobSpec
from ..storage import open_store
from . import EXIT_INVALID_INPUT, EXIT_OK, EXIT_UNKNOWN, RunnerFailure
from .core import Context, Events, install_signal_handlers, load_composition, pin_threads, publish
from .core import stage_inputs as _stage

HANDLERS = {"swmm.simulation": "hydrocouple_platform.runner.swmm",
            "gym.evaluate": "hydrocouple_platform.runner.gym"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hydrocouple_platform.runner")
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--attempt-number", required=True, type=int)
    parser.add_argument("--store", required=True)
    parser.add_argument("--workdir", required=True, type=Path)
    args = parser.parse_args(argv)

    events = Events()
    try:
        spec = JobSpec.model_validate_json(args.spec.read_text(encoding="utf-8"))
    except Exception as exc:
        events.emit("lifecycle", "failed", f"the job spec is invalid: {exc}")
        return EXIT_INVALID_INPUT

    pin_threads(spec.runtime.threads)
    ctx = Context(spec=spec, attempt_id=args.attempt_id, attempt_number=args.attempt_number,
                  workdir=args.workdir, store=open_store(args.store), events=events)
    install_signal_handlers(ctx)
    ctx.outputs.mkdir(parents=True, exist_ok=True)
    workload = spec.workload
    events.emit("lifecycle", "started", f"{workload.workflow}@{workload.workflow_version}")
    try:
        module_name = HANDLERS.get(spec.workload.workflow)
        if module_name is None:
            raise RunnerFailure(EXIT_INVALID_INPUT,
                                f"no handler for workflow {spec.workload.workflow}")
        import importlib

        handler = importlib.import_module(module_name)
        inputs_used = _stage(ctx)
        composition = load_composition(ctx)
        composition_sha = hashlib.sha256(
            (ctx.inputs / spec.inputs.composition).read_bytes()).hexdigest()
        result = handler.run(ctx, composition)
        publish(ctx, result, inputs_used, composition_sha)
        return EXIT_OK
    except RunnerFailure as failure:
        _diagnostics(ctx, {"message": str(failure), "exit_code": failure.exit_code})
        events.emit("lifecycle", "failed", str(failure))
        return failure.exit_code
    except Exception as exc:  # unclassified: reported, not hidden
        _diagnostics(ctx, {"message": str(exc), "traceback": traceback.format_exc()[-4000:]})
        events.emit("lifecycle", "failed", f"{type(exc).__name__}: {exc}")
        return EXIT_UNKNOWN


def _diagnostics(ctx: Context, data: dict) -> None:
    """Keep failure diagnostics beside the attempt's outputs for review."""
    try:
        path = ctx.outputs / "diagnostics.json"
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        ctx.store.put_file(ctx.output_prefix + "diagnostics.json", path)
    except Exception:
        pass


if __name__ == "__main__":
    sys.exit(main())
