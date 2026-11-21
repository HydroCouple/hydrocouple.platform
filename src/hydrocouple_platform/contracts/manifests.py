"""Revision, attempt and provenance manifests, and the run summary.

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

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    model_serializer,
    model_validator,
)

from .jobs import Actor

REVISION_MANIFEST_SCHEMA = "hydrocouple.platform/revision-manifest/1"
ATTEMPT_MANIFEST_SCHEMA = "hydrocouple.platform/attempt-manifest/1"
PROVENANCE_PROFILE = "hydrocouple.platform/provenance/1"
RUN_SUMMARY_SCHEMA = "hydrocouple.platform/run-summary/1"

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


class DerivedFrom(_Model):
    """The revision a revision was made from, and the study evaluation that chose it."""

    revision_id: str
    revision_manifest_sha256: str = Field(pattern=_SHA)
    study_id: str | None = None
    evaluation: int | None = Field(default=None, ge=1)
    decisions: dict[str, list[float]] | None = Field(
        default=None, description="The evaluation's design, by dimension key")


class ComponentInstance(_Model):
    """One component of a composition of more than the generated single model."""

    id: str = Field(min_length=1, description="The component's id in the composition")
    component_info_id: str
    role: str = Field(description="Its configuration's role (complete_model, split_network, ...)")
    model: SafePath = Field(description="The model file it runs, a path in the package")


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
    derived_from: DerivedFrom | None = Field(
        default=None, description="Lineage of a revision made from another; absent otherwise.")
    component_instances: list[ComponentInstance] | None = Field(
        default=None, description="The components of an authored composition, with the role "
                                  "and model each runs; absent for a generated single model.")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_serializer(mode="wrap")
    def _no_lineage_no_key(self, handler):
        # Absent, not null, so the manifests written before these fields keep their digests.
        data = handler(self)
        if self.derived_from is None:
            data.pop("derived_from", None)
        if self.component_instances is None:
            data.pop("component_instances", None)
        return data

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
    STATE = "state"  # saved model state a later run resumes from
    FORECAST = "forecast"


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


class ResumedFrom(_Model):
    """The saved state an attempt resumed from: an artifact of an earlier run's attempt."""

    run_id: str
    attempt_id: str
    path: str
    sha256: str = Field(pattern=_SHA)


class ComponentRecord(_Model):
    """A component a composition run ran, and how it ended."""

    id: str
    component_info_id: str
    role: str
    final_status: str = Field(description="The host's final status for it (Finished, Failed, ...)")
    qualification: str = Field(description="The engine's word for the role: qualified or "
                                           "experimental")


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
    resumed_from: ResumedFrom | None = Field(
        default=None, description="The saved state of an earlier run this attempt resumed from")
    components: list[ComponentRecord] | None = Field(
        default=None, description="The components a composition run ran; absent for a run of "
                                  "one model")

    @model_serializer(mode="wrap")
    def _no_resume_no_key(self, handler):
        # Absent, not null, so attempt manifests written before these fields keep their digests.
        data = handler(self)
        if self.resumed_from is None:
            data.pop("resumed_from", None)
        if self.components is None:
            data.pop("components", None)
        return data

    def to_prov(self) -> dict[str, Any]:
        act = f"hcp:attempt/{self.attempt_id}"
        used = {
            f"u{i}": {"prov:activity": act, "prov:entity": f"hcp:input/{sha}"}
            for i, sha in enumerate(sorted(self.inputs.values()))
        }
        human = f"hcp:human/{self.actor.human_subject}"
        doc: dict[str, Any] = {
            "prefix": {"hcp": "https://hydrocouple.org/platform/ns#"},
            "activity": {
                act: {
                    "prov:startTime": self.started_at.isoformat(),
                    "prov:endTime": self.finished_at.isoformat(),
                    "hcp:workflow": f"{self.workflow}@{self.workflow_version}",
                }
            },
            "entity": {f"hcp:input/{sha}": {"hcp:path": p} for p, sha in self.inputs.items()},
            "agent": {human: {}},
            "used": used,
            "wasAssociatedWith": {"w0": {"prov:activity": act, "prov:agent": human}},
        }
        if self.actor.agent_session_id:
            # An AI agent asked for the work, acting for the person who started its session.
            session = f"hcp:agent-session/{self.actor.agent_session_id}"
            described = {"hcp:client": self.actor.agent_client,
                         "hcp:clientVersion": self.actor.agent_client_version,
                         "hcp:model": self.actor.model,
                         "hcp:toolCall": self.actor.tool_call_id}
            doc["agent"][session] = {k: v for k, v in described.items() if v is not None}
            doc["wasAssociatedWith"]["w1"] = {"prov:activity": act, "prov:agent": session}
            doc["actedOnBehalfOf"] = {
                "d0": {"prov:delegate": session, "prov:responsible": human, "prov:activity": act}
            }
        return doc


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

    def to_prov(self) -> dict[str, Any]:
        """The provenance in PROV-JSON terms, each artifact an entity the attempt generated."""
        doc = self.provenance.to_prov()
        act = f"hcp:attempt/{self.provenance.attempt_id}"
        doc["wasGeneratedBy"] = {}
        for i, a in enumerate(self.artifacts):
            entity = f"hcp:artifact/{a.sha256}"
            doc["entity"][entity] = {"hcp:path": a.path, "hcp:kind": a.kind.value}
            doc["wasGeneratedBy"][f"g{i}"] = {"prov:entity": entity, "prov:activity": act}
        return doc


class SummaryColumn(_Model):
    """What one column of a summary table reports."""

    catalog_path: str | None = Field(
        default=None, description="Engine catalog path of the quantity, where it has one.")
    unit: str | None = Field(
        default=None,
        description="Unit as the engine labels it for this model; absent when it declares none.")


class SummaryTable(_Model):
    columns: dict[str, SummaryColumn] = Field(default_factory=dict)
    rows: list[dict[str, float | int | str | None]]


class RunSummary(_Model):
    """The engine's end-of-run report as data: the tables of the text report.

    It names model elements, so it is an artifact in the target's storage and is
    subject to the target's egress policy; it is never sent in events.
    """

    schema_: Literal["hydrocouple.platform/run-summary/1"] = Field(
        default=RUN_SUMMARY_SCHEMA, alias="schema"
    )
    engine: str | None = None
    unit_system: str | None = None
    flow_units: str | None = None
    tables: dict[str, SummaryTable]

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
