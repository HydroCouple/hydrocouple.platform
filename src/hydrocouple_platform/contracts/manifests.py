"""Revision, attempt and provenance manifests.

The HydroCouple run manifest (``run-1.0.json``) is closed to additional
properties, so the platform never extends it. The runner writes a valid run
manifest and, beside it, an attempt manifest that references it by checksum
and adds the execution half of provenance.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from .jobs import Actor

REVISION_MANIFEST_SCHEMA = "hydrocouple.platform/revision-manifest/1"
ATTEMPT_MANIFEST_SCHEMA = "hydrocouple.platform/attempt-manifest/1"
PROVENANCE_PROFILE = "hydrocouple.platform/provenance/1"

_SHA = r"^[0-9a-f]{64}$"


def check_safe_path(path: str) -> str:
    """Accept only relative POSIX paths that cannot escape their root.

    Rejects absolute paths, drive letters, backslashes, NUL, empty, '.' and
    '..' segments, and trailing slashes. Packaging and storage both rely on it.
    """
    if not path or len(path) > 1024:
        raise ValueError("path must be 1 to 1024 characters")
    if "\\" in path or "\x00" in path:
        raise ValueError(f"path {path!r} contains a backslash or NUL")
    if path.startswith("/") or (len(path) > 1 and path[1] == ":"):
        raise ValueError(f"path {path!r} is absolute")
    for segment in path.split("/"):
        if segment in ("", ".", ".."):
            raise ValueError(f"path {path!r} has an empty, '.' or '..' segment")
    return path


SafePath = Annotated[str, AfterValidator(check_safe_path)]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


def canonical_json(data: Any) -> bytes:
    """Deterministic JSON bytes: sorted keys, no whitespace, UTF-8."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256_of(data: Any) -> str:
    return hashlib.sha256(canonical_json(data)).hexdigest()


class FileRole(str, Enum):
    COMPOSITION = "composition"
    MODEL = "model"
    INPUT = "input"
    SIDECAR = "sidecar"


class ManifestFile(_Model):
    path: SafePath
    sha256: str = Field(pattern=_SHA)
    size: int = Field(ge=0)
    role: FileRole
    source: str | None = Field(
        default=None, description="Original reference as written in the model, for review."
    )


class RevisionManifest(_Model):
    schema_: Literal["hydrocouple.platform/revision-manifest/1"] = Field(
        default=REVISION_MANIFEST_SCHEMA, alias="schema"
    )
    composition: SafePath = "composition.json"
    files: list[ManifestFile] = Field(min_length=1)
    components: list[str] = Field(description="component_info_id of every component")
    total_bytes: int = Field(ge=0)
    created_at: datetime
    packaged_by: str
    findings: list[str] = Field(
        default_factory=list, description="Non-blocking observations recorded at packaging."
    )

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    def digest(self) -> str:
        return sha256_of(self.model_dump(mode="json", by_alias=True))


class ArtifactKind(str, Enum):
    RUN_MANIFEST = "run_manifest"
    REPORT = "report"
    BINARY_OUTPUT = "binary_output"
    LOG = "log"
    SUMMARY = "summary"
    EVALUATIONS = "evaluations"
    OTHER = "other"


class ArtifactEntry(_Model):
    path: SafePath
    sha256: str = Field(pattern=_SHA)
    size: int = Field(ge=0)
    kind: ArtifactKind
    format: str


class RuntimeVersions(_Model):
    platform: str
    python: str
    engine: str | None = None
    image: str | None = None
    host: str | None = None


class Provenance(_Model):
    """Execution provenance; ``to_prov`` renders it in W3C PROV-JSON terms."""

    profile: Literal["hydrocouple.platform/provenance/1"] = PROVENANCE_PROFILE
    run_id: str
    job_id: str
    attempt_id: str
    attempt_number: int
    revision_id: str
    revision_manifest_sha256: str = Field(pattern=_SHA)
    composition_sha256: str = Field(pattern=_SHA)
    inputs: dict[str, str] = Field(description="Package path to sha256 of every input used")
    workflow: str
    workflow_version: str
    parameters: dict[str, Any]
    seeds: dict[str, int] = Field(default_factory=dict)
    threads: int
    runtime: RuntimeVersions
    actor: Actor
    started_at: datetime
    finished_at: datetime

    def to_prov(self) -> dict[str, Any]:
        act = f"hcp:attempt/{self.attempt_id}"
        used = {
            f"u{i}": {"prov:activity": act, "prov:entity": f"hcp:input/{sha}"}
            for i, sha in enumerate(sorted(self.inputs.values()))
        }
        return {
            "prefix": {"hcp": "https://hydrocouple.org/platform/ns#"},
            "activity": {
                act: {
                    "prov:startTime": self.started_at.isoformat(),
                    "prov:endTime": self.finished_at.isoformat(),
                    "hcp:workflow": f"{self.workflow}@{self.workflow_version}",
                }
            },
            "entity": {f"hcp:input/{sha}": {"hcp:path": p} for p, sha in self.inputs.items()},
            "agent": {f"hcp:human/{self.actor.human_subject}": {}},
            "used": used,
            "wasAssociatedWith": {
                "w0": {"prov:activity": act, "prov:agent": f"hcp:human/{self.actor.human_subject}"}
            },
        }


class AttemptManifest(_Model):
    schema_: Literal["hydrocouple.platform/attempt-manifest/1"] = Field(
        default=ATTEMPT_MANIFEST_SCHEMA, alias="schema"
    )
    run_manifest: SafePath = "run.json"
    run_manifest_sha256: str = Field(pattern=_SHA)
    status: Literal["completed", "failed", "stopped"]
    artifacts: list[ArtifactEntry]
    quality: dict[str, Any] = Field(default_factory=dict)
    provenance: Provenance

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _unique_paths(self) -> AttemptManifest:
        paths = [a.path for a in self.artifacts]
        if len(set(paths)) != len(paths):
            raise ValueError("the attempt manifest lists an artifact path twice")
        return self
