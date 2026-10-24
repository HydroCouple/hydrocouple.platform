"""Runner core: stage inputs, run a handler, publish outputs and manifests."""

from __future__ import annotations

import json
import os
import platform
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any

import jsonschema

from .. import __version__
from ..contracts.jobs import JobSpec
from ..contracts.manifests import (
    ArtifactEntry,
    ArtifactKind,
    AttemptManifest,
    Provenance,
    RuntimeVersions,
    canonical_json,
    sha256_of,
)
from ..storage import ObjectExists, StorageBackend, sha256_file
from . import (
    EVENT_PREFIX,
    EXIT_CANCELLED,
    EXIT_INVALID_INPUT,
    EXIT_MISSING_DEPENDENCY,
    EXIT_TRANSFER,
    RunnerFailure,
)


def load_schema(name: str) -> dict:
    files = resources.files("hydrocouple_platform") / "schemas" / "hydrocouple" / name
    return json.loads(files.read_text(encoding="utf-8"))


class Events:
    """Writes runner events as JSON lines on stdout for the agent to relay."""

    def __init__(self, stream=None):
        self._stream = stream or sys.stdout
        self._sequence = 0
        self._lock = threading.Lock()
        self._t0 = time.monotonic()

    def emit(self, kind: str, phase: str | None = None, message: str | None = None,
             completed: float | None = None, total: float | None = None,
             unit: str | None = None) -> None:
        with self._lock:
            event = {"event_id": uuid.uuid4().hex, "sequence": self._sequence, "kind": kind,
                     "phase": phase, "completed": completed, "total": total, "unit": unit,
                     "message": message, "occurred_at": datetime.now(timezone.utc).isoformat(),
                     "agent_clock_seconds": None}
            self._sequence += 1
            self._stream.write(EVENT_PREFIX + json.dumps(event) + "\n")
            self._stream.flush()


@dataclass
class Context:
    spec: JobSpec
    attempt_id: str
    attempt_number: int
    workdir: Path
    store: StorageBackend
    events: Events
    cancel: threading.Event = field(default_factory=threading.Event)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Values the agent passed on stdin, kept out of the environment (the gateway token).
    secrets: dict[str, str] = field(default_factory=dict, repr=False)

    @property
    def inputs(self) -> Path:
        return self.workdir / "inputs"

    @property
    def outputs(self) -> Path:
        return self.workdir / "outputs"

    @property
    def output_prefix(self) -> str:
        return f"{self.spec.outputs.storage_prefix}{self.attempt_id}/"

    def check_cancel(self) -> None:
        if self.cancel.is_set():
            raise RunnerFailure(EXIT_CANCELLED, "cancelled by the agent")


@dataclass
class HandlerResult:
    status: str  # completed | failed | stopped
    components: list[dict[str, Any]]
    quality: dict[str, Any]
    engine_version: str | None
    artifacts: list[tuple[str, ArtifactKind, str]]  # (path under outputs, kind, format)


Handler = Callable[[Context, dict], HandlerResult]


def stage_inputs(ctx: Context) -> dict[str, str]:
    """Download every input and check it against the job spec; returns path to sha256."""
    ctx.events.emit("lifecycle", "staging", f"{len(ctx.spec.inputs.files)} files")
    used: dict[str, str] = {}
    for f in ctx.spec.inputs.files:
        ctx.check_cancel()
        dest = ctx.inputs / f.path
        try:
            ctx.store.get_file(ctx.spec.inputs.storage_prefix + f.path, dest)
        except FileNotFoundError:
            raise RunnerFailure(EXIT_MISSING_DEPENDENCY, f"input {f.path} is missing") from None
        except OSError as exc:
            raise RunnerFailure(EXIT_TRANSFER, f"could not download {f.path}: {exc}") from None
        digest = sha256_file(dest)
        if digest != f.sha256 or dest.stat().st_size != f.size:
            raise RunnerFailure(EXIT_TRANSFER, f"input {f.path} does not match its checksum")
        used[f.path] = digest
    return used


def load_composition(ctx: Context) -> dict:
    path = ctx.inputs / ctx.spec.inputs.composition
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        jsonschema.validate(doc, load_schema("composition-1.1.json"))
    except (OSError, ValueError, jsonschema.ValidationError) as exc:
        raise RunnerFailure(EXIT_INVALID_INPUT, f"the composition is invalid: {exc}") from None
    return doc


def _upload(ctx: Context, rel: str, source: Path) -> None:
    key = ctx.output_prefix + rel
    try:
        ctx.store.put_file(key, source, if_absent=True)
    except ObjectExists:
        # A restarted runner for the same attempt may find its own earlier upload.
        stat = ctx.store.stat(key)
        if stat is None or stat[1] != sha256_file(source):
            raise RunnerFailure(EXIT_TRANSFER, f"{key} already exists with other content") from None
    except OSError as exc:
        raise RunnerFailure(EXIT_TRANSFER, f"could not upload {rel}: {exc}") from None


def publish(ctx: Context, result: HandlerResult, inputs_used: dict[str, str],
            composition_sha: str) -> str:
    """Write run.json and attempt.json, upload every artifact; returns the manifest sha256."""
    ctx.events.emit("lifecycle", "publishing")
    finished = datetime.now(timezone.utc)
    run_manifest = {
        "schema_version": "1.0",
        "run": {"id": f"{ctx.spec.run_id}/{ctx.attempt_id}", "status": result.status,
                "started": ctx.started_at.isoformat(), "finished": finished.isoformat(),
                "wall_seconds": round((finished - ctx.started_at).total_seconds(), 3),
                "host": platform.node() or "unknown"},
        "composition": {"path": ctx.spec.inputs.composition, "sha256": composition_sha},
        "components": result.components,
        # SWMM's native .out/.rpt are not formats the run-1.0 catalog can describe;
        # they are listed in the attempt manifest instead.
        "results": [],
    }
    jsonschema.validate(run_manifest, load_schema("run-1.0.json"))
    run_path = ctx.outputs / "run.json"
    run_path.write_bytes(json.dumps(run_manifest, indent=2).encode() + b"\n")

    entries: list[ArtifactEntry] = []
    for rel, kind, fmt in [("run.json", ArtifactKind.RUN_MANIFEST, "json"), *result.artifacts]:
        source = ctx.outputs / rel
        ctx.check_cancel()
        _upload(ctx, rel, source)
        entries.append(ArtifactEntry(path=rel, sha256=sha256_file(source),
                                     size=source.stat().st_size, kind=kind, format=fmt))

    spec = ctx.spec
    manifest = AttemptManifest(
        run_manifest_sha256=entries[0].sha256,
        status=result.status,
        artifacts=entries,
        quality=result.quality,
        provenance=Provenance(
            run_id=spec.run_id, job_id=spec.job_id, attempt_id=ctx.attempt_id,
            attempt_number=ctx.attempt_number, revision_id=spec.inputs.revision_id,
            revision_manifest_sha256=spec.inputs.manifest_sha256,
            composition_sha256=composition_sha, inputs=inputs_used,
            workflow=spec.workload.workflow, workflow_version=spec.workload.workflow_version,
            parameters=spec.workload.parameters, threads=spec.runtime.threads,
            runtime=RuntimeVersions(platform=__version__, python=platform.python_version(),
                                    engine=result.engine_version, image=spec.runtime.image,
                                    host=platform.platform()),
            actor=spec.actor, started_at=ctx.started_at, finished_at=finished,
        ),
    ).model_dump(mode="json", by_alias=True)
    manifest_path = ctx.outputs / "attempt.json"
    manifest_path.write_bytes(canonical_json(manifest))
    _upload(ctx, "attempt.json", manifest_path)
    digest = sha256_of(manifest)
    ctx.events.emit("lifecycle", "published", digest)
    return digest


def install_signal_handlers(ctx: Context) -> None:
    def _stop(signum, _frame):
        ctx.cancel.set()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)


def pin_threads(threads: int) -> None:
    """Pin solver threads before the engine loads, so retries reproduce the result."""
    os.environ["OMP_NUM_THREADS"] = str(threads)
