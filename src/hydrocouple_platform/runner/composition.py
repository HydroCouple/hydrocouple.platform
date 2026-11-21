"""``composition.run@1``: an authored composition of OpenSWMM engine components, run by the
target's composition host, HydroCoupleComposer in its headless mode.

The host comes from the runner's environment, which the agent sets from its configuration:
``HYDROCOUPLE_COMPOSER`` (the executable) and ``HYDROCOUPLE_COMPONENT_PATH`` (the directories
holding the engine component module, ``os.pathsep`` separated). The runner writes a run copy of
the composition beside the packaged one, with each component's ``output_directory`` in its
outputs (``components/<id>``) and the host's run manifest at ``host/run-manifest.json``; runs
the host with JSON progress and relays its events; and checks that every component wrote the
outputs its role writes. The attempt names every component, its role, how it ended and the
engine's word for the role (the material split is experimental in the engine's own terms).
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from pathlib import Path

from ..contracts.manifests import ArtifactKind
from ..workflows import OPENSWMM_ENGINE_COMPONENT
from . import EXIT_ENGINE, EXIT_INVALID_INPUT, EXIT_MISSING_DEPENDENCY, RunnerFailure
from .core import Context, HandlerResult

HOST_EVENT = "HYDROCOUPLE_EVENT "
HOST_MANIFEST = "host/run-manifest.json"
# The engine's word for each role: docs/SPLIT_WATER_PACKET.md qualifies the water split for one
# Composer process; docs/SPLIT_MATERIAL_PACKET.md calls the material split experimental.
QUALIFICATION = {"complete_model": "qualified", "network_only": "qualified",
                 "split_network": "qualified", "integrated2d": "qualified",
                 "split_material_network": "experimental",
                 "split_material_surface": "experimental"}
SURFACE_ROLES = {"integrated2d", "split_material_surface"}
# What a role writes besides configuration.json and metadata.json: (file, kind, format).
REPORT = ("model.rpt", ArtifactKind.REPORT, "swmm_rpt")
NETWORK_OUT = ("model.out", ArtifactKind.BINARY_OUTPUT, "swmm_out")
SURFACE_OUT = ("model.2d.h5", ArtifactKind.BINARY_OUTPUT, "openswmm_2d_hdf5")


def _host() -> tuple[str, list[str]]:
    executable = os.environ.get("HYDROCOUPLE_COMPOSER", "")
    paths = [p for p in os.environ.get("HYDROCOUPLE_COMPONENT_PATH", "").split(os.pathsep) if p]
    if not executable or not Path(executable).is_file() or not paths:
        raise RunnerFailure(EXIT_MISSING_DEPENDENCY,
                            "this target has no composition host (HYDROCOUPLE_COMPOSER and "
                            "HYDROCOUPLE_COMPONENT_PATH are not set)")
    return executable, paths


def _required(role: str, config: dict) -> list[tuple[str, ArtifactKind, str]]:
    if role in SURFACE_ROLES:
        return [REPORT, SURFACE_OUT] if config.get("surface_results", True) else [REPORT]
    return [REPORT, NETWORK_OUT]


def _components(composition: dict) -> list[dict]:
    components = composition.get("components") or []
    if not components:
        raise RunnerFailure(EXIT_INVALID_INPUT, "the composition has no components")
    for c in components:
        info = (c.get("info") or {}).get("component_info_id")
        if info != OPENSWMM_ENGINE_COMPONENT:
            raise RunnerFailure(EXIT_INVALID_INPUT, f"component {c.get('id')!r} is {info!r}; "
                                "only the OpenSWMM engine component is approved")
        role = ((c.get("arguments") or {}).get("configuration") or {}).get("role")
        if role not in QUALIFICATION:
            raise RunnerFailure(EXIT_INVALID_INPUT,
                                f"component {c.get('id')!r} has role {role!r}; known roles: "
                                + ", ".join(sorted(QUALIFICATION)))
    return components


def _relay(ctx: Context, stream, last: dict) -> None:
    """Relay the host's JSON progress as run events; keep the last of each kind in ``last``."""
    for raw in stream:
        line = raw.decode("utf-8", "replace").rstrip("\n")
        if not line.startswith(HOST_EVENT):
            last.setdefault("log", []).append(line)
            continue
        try:
            event = json.loads(line[len(HOST_EVENT):])
        except ValueError:
            continue
        kind = event.get("event")
        last[kind] = event
        if kind == "progress":
            clocks = ", ".join(f"{c['id']} {c.get('current_julian_day')}"
                               for c in event.get("components") or [])
            ctx.events.emit("progress", "running", f"step {event.get('step')}: {clocks}",
                            completed=float(event.get("step") or 0), unit="steps")
        elif kind in ("started", "finished"):
            ctx.events.emit("lifecycle", f"host {kind}", event.get("message"))


def run(ctx: Context, composition: dict) -> HandlerResult:
    executable, paths = _host()
    components = _components(composition)
    ctx.outputs.mkdir(parents=True, exist_ok=True)
    (ctx.outputs / "host").mkdir(exist_ok=True)
    (ctx.outputs / "components").mkdir(exist_ok=True)
    run_copy = json.loads(json.dumps(composition))
    for c in run_copy["components"]:
        c["arguments"]["configuration"]["output_directory"] = str(
            ctx.outputs / "components" / c["id"])
    run_copy["run"] = {**(run_copy.get("run") or {}), "manifest": str(ctx.outputs / HOST_MANIFEST)}
    # Beside the packaged composition, so the components' model paths resolve as packaged.
    run_file = ctx.inputs / f"{Path(ctx.spec.inputs.composition).stem}.run.json"
    run_file.write_text(json.dumps(run_copy, indent=2), encoding="utf-8")

    command = [executable, "--run", str(run_file), "--json-progress"]
    for p in paths:
        command += ["--components", p]
    env = {**os.environ, "QT_QPA_PLATFORM": "offscreen"}
    began = time.monotonic()
    ctx.events.emit("lifecycle", "running", f"{len(components)} components on "
                    f"{Path(executable).name}")
    process = subprocess.Popen(command, cwd=str(ctx.inputs), stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, env=env, start_new_session=True)
    last: dict = {}
    reader = threading.Thread(target=_relay, args=(ctx, process.stdout, last), daemon=True)
    reader.start()
    try:
        while process.poll() is None:
            if ctx.cancel.is_set():
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                ctx.check_cancel()
            time.sleep(0.2)
    finally:
        reader.join(timeout=10)
    log = "\n".join(last.get("log", []))
    (ctx.outputs / "host" / "composer.log").write_text(log + "\n", encoding="utf-8")
    finished = last.get("finished") or {}
    if process.returncode != 0 or not finished.get("succeeded"):
        detail = finished.get("message") or (log.strip().splitlines() or ["no output"])[-1]
        raise RunnerFailure(EXIT_ENGINE, f"the composition failed (host exit "
                            f"{process.returncode}): {detail}")
    manifest_path = ctx.outputs / HOST_MANIFEST
    if not manifest_path.is_file():
        raise RunnerFailure(EXIT_ENGINE, "the host wrote no run manifest")
    host_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    status = {c["id"]: c for c in host_manifest.get("components") or []}

    artifacts = [(HOST_MANIFEST, ArtifactKind.RUN_MANIFEST, "json"),
                 ("host/composer.log", ArtifactKind.LOG, "text")]
    records, run_components = [], []
    for c in components:
        config = c["arguments"]["configuration"]
        role = config["role"]
        folder = ctx.outputs / "components" / c["id"]
        missing = [name for name, _, _ in _required(role, config) if not (folder / name).is_file()]
        ended = status.get(c["id"], {})
        if missing:
            raise RunnerFailure(EXIT_ENGINE, f"component {c['id']} ({role}) wrote no "
                                + ", ".join(missing))
        kinds = {name: (kind, fmt) for name, kind, fmt in _required(role, config)}
        for f in sorted(p for p in folder.rglob("*") if p.is_file()):
            rel = f.relative_to(ctx.outputs).as_posix()
            kind, fmt = kinds.get(f.name, (ArtifactKind.OTHER,
                                           "json" if f.suffix == ".json" else "other"))
            artifacts.append((rel, kind, fmt))
        records.append({"id": c["id"], "component_info_id": OPENSWMM_ENGINE_COMPONENT,
                        "role": role, "final_status": ended.get("final_status", "Unknown"),
                        "qualification": QUALIFICATION[role]})
        run_components.append({"id": c["id"], "final_status": ended.get("final_status",
                                                                         "Unknown"),
                               "errors": ended.get("errors", [])})
    return HandlerResult(
        status="completed",
        components=run_components,
        quality={"host": {"name": "HydroCoupleComposer", "executable": Path(executable).name,
                          "message": finished.get("message"),
                          "diagnostics": finished.get("diagnostics") or [],
                          "composition_sha256": (last.get("started") or {}).get(
                              "composition_sha256")},
                 "experimental": sorted(r["id"] for r in records
                                        if r["qualification"] == "experimental"),
                 "seconds": round(time.monotonic() - began, 3)},
        engine_version=None,  # the host runs its own engine component module
        artifacts=artifacts,
        provenance_components=records,
    )
