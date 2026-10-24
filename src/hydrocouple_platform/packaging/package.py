"""Build an immutable revision package from a SWMM model on the user's machine.

The original files are never modified. The packaged ``.inp`` is a rewritten
copy whose references point inside the package, written to a staging folder
the caller chooses (so it can be reviewed). Uploading is left to
``PlatformClient.upload_revision``.
"""

from __future__ import annotations

import json
import os
import platform as _platform
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .. import __version__
from ..contracts.manifests import (
    FileRole,
    ManifestFile,
    RevisionManifest,
    canonical_json,
    check_safe_path,
)
from ..storage import sha256_file
from ..workflows import OPENSWMM_ENGINE_COMPONENT
from . import inp as inpscan

MODEL_DIR = "model"


class PackagingError(Exception):
    """The model cannot be packaged; ``problems`` lists every reason found."""

    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


@dataclass
class Package:
    manifest: RevisionManifest
    staging_dir: Path
    _sources: dict[str, Path] = field(default_factory=dict)

    def sources(self) -> dict[str, Path]:
        """Package path to the local file holding its bytes."""
        return dict(self._sources)


def _stamp(path: Path) -> tuple[int, int]:
    st = path.stat()
    return st.st_size, st.st_mtime_ns


def package_swmm_model(
    inp_path: str | Path,
    staging_dir: str | Path,
    *,
    max_files: int = 5000,
    max_bytes: int = 20 * 1024**3,
) -> Package:
    """Package one SWMM model as a one-component HydroCouple composition."""
    inp_path = Path(inp_path).resolve()
    model_dir = inp_path.parent
    staging = Path(staging_dir).resolve()
    staging.mkdir(parents=True, exist_ok=True)
    text = inp_path.read_text(encoding="utf-8")

    problems: list[str] = []
    findings: list[str] = []
    sources: dict[str, Path] = {}
    roles: dict[str, tuple[FileRole, str | None]] = {}
    replacements: list[tuple[inpscan.Reference, str]] = []
    external_index = 0

    for ref in inpscan.scan(text):
        if ref.kind == inpscan.RefKind.PLUGIN:
            problems.append(f"[PLUGINS] line {ref.line + 1} loads native library {ref.text!r}; "
                            "native plugins must come from an approved runner image")
            continue
        if ref.kind == inpscan.RefKind.OUTPUT:
            # Outputs are written inside the model folder at run time and collected.
            name = PurePosixPath(Path(ref.text).name).as_posix()
            replacements.append((ref, name))
            findings.append(f"[{ref.section}] SAVE {ref.text!r} is written as {name!r} and "
                            "collected with the run's outputs")
            continue
        resolved, decorator = inpscan.resolve(ref, model_dir)
        if not resolved.is_file():
            problems.append(f"[{ref.section}] line {ref.line + 1} references {ref.text!r}, "
                            "which does not exist")
            continue
        real = resolved.resolve()
        inside = model_dir in real.parents
        if not inside and not Path(ref.text).is_absolute() and ".." not in Path(ref.text).parts:
            problems.append(f"[{ref.section}] line {ref.line + 1}: {ref.text!r} escapes the "
                            f"model folder through a symbolic link to {str(real)!r}")
            continue
        if inside:
            rel = real.relative_to(model_dir).as_posix()
        else:
            # Named explicitly outside the model folder: copied under external/.
            rel = f"external/{external_index}-{real.name}"
            external_index += 1
            findings.append(f"{ref.text!r} is outside the model folder; packaged as {rel!r}")
        package_path = f"{MODEL_DIR}/{rel}"
        try:
            check_safe_path(package_path)
        except ValueError as exc:
            problems.append(str(exc))
            continue
        existing = sources.get(package_path)
        if existing is not None and existing != real:
            problems.append(f"two references map to {package_path}")
            continue
        sources[package_path] = real
        roles[package_path] = (FileRole.INPUT, ref.text)
        replacements.append((ref, rel + decorator))

    if problems:
        raise PackagingError(problems)

    packaged_inp = staging / MODEL_DIR / "model.inp"
    packaged_inp.parent.mkdir(parents=True, exist_ok=True)
    packaged_inp.write_text(inpscan.rewrite(text, replacements), encoding="utf-8")
    sources[f"{MODEL_DIR}/model.inp"] = packaged_inp
    roles[f"{MODEL_DIR}/model.inp"] = (FileRole.MODEL, inp_path.name)

    composition = {
        "schema_version": "1.1",
        "metadata": {"caption": inp_path.stem,
                     "created": datetime.now(timezone.utc).isoformat()},
        "components": [{
            "id": "swmm",
            "caption": inp_path.stem,
            "info": {"component_info_id": OPENSWMM_ENGINE_COMPONENT},
            "arguments": {"configuration": {
                "schema": "openswmm.component/1",
                "role": "complete_model",
                "model": f"{MODEL_DIR}/model.inp",
                "output_directory": "outputs",
            }},
        }],
        "connections": [],
        "workflow": {"strategy": "none"},
    }
    composition_file = staging / "composition.json"
    composition_file.write_bytes(json.dumps(composition, indent=2).encode() + b"\n")
    sources["composition.json"] = composition_file
    roles["composition.json"] = (FileRole.COMPOSITION, None)

    if len(sources) > max_files:
        raise PackagingError([f"the package has {len(sources)} files; the limit is {max_files}"])

    files: list[ManifestFile] = []
    for path, source in sorted(sources.items()):
        before = _stamp(source)
        digest = sha256_file(source)
        after = _stamp(source)
        if after != before and sha256_file(source) != digest:
            # Only a real content change fails; some filesystems settle timestamps late.
            raise PackagingError([f"{source} changed while it was being packaged; try again"])
        role, original = roles[path]
        files.append(ManifestFile(path=path, sha256=digest, size=after[0], role=role,
                                  source=original))
    total = sum(f.size for f in files)
    if total > max_bytes:
        raise PackagingError([f"the package is {total} bytes; the limit is {max_bytes}"])

    manifest = RevisionManifest(
        files=files, components=[OPENSWMM_ENGINE_COMPONENT], total_bytes=total,
        created_at=datetime.now(timezone.utc),
        packaged_by=f"hydrocouple.platform/{__version__} on {_platform.system()} "
                    f"{_platform.machine()} ({os.sep!r} paths)",
        findings=findings,
    )
    (staging / "revision-manifest.json").write_bytes(
        canonical_json(manifest.model_dump(mode="json", by_alias=True)))
    return Package(manifest=manifest, staging_dir=staging, _sources=sources)
