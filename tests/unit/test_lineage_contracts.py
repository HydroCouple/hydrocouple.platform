"""Phase 2 contracts: a revision's lineage, the study manifest, and the approval views."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from importlib import resources

import jsonschema
import pytest
from pydantic import ValidationError

from conftest import DATA
from hydrocouple_platform.contracts.manifests import (
    DerivedFrom,
    RevisionManifest,
    canonical_json,
    sha256_of,
)
from hydrocouple_platform.contracts.requests import AgentSessionView, ApprovalView
from hydrocouple_platform.contracts.studies import StudyManifest
from hydrocouple_platform.packaging import package_swmm_model


def _schema(name: str) -> dict:
    files = resources.files("hydrocouple_platform") / "schemas" / "platform"
    return json.loads((files / name).read_text(encoding="utf-8"))


def test_a_manifest_without_lineage_keeps_its_digest(artifact_dir):
    package = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "package")
    written = (artifact_dir / "package" / "revision-manifest.json").read_bytes()
    document = json.loads(written)
    assert "derived_from" not in document
    reread = RevisionManifest.model_validate(document)
    assert canonical_json(reread.model_dump(mode="json", by_alias=True)) == written
    assert reread.digest() == package.manifest.digest() == sha256_of(document)


def test_a_derived_revision_names_its_base_and_study(artifact_dir):
    package = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "package")
    lineage = DerivedFrom(revision_id="rev_base", revision_manifest_sha256="a" * 64,
                          study_id="std_1", evaluation=3, decisions={"subcatchment.width": [300.0]})
    derived = package.manifest.model_copy(update={"derived_from": lineage})
    document = derived.model_dump(mode="json", by_alias=True)
    assert document["derived_from"]["study_id"] == "std_1"
    jsonschema.validate(document, _schema("revision-manifest-1.json"))
    assert RevisionManifest.model_validate(document) == derived
    assert derived.digest() != package.manifest.digest()
    with pytest.raises(ValidationError):
        DerivedFrom(revision_id="rev_base", revision_manifest_sha256="not-a-hash")


def test_two_runs_of_one_study_compare_without_their_runtime():
    record = {"evaluation": 1, "decisions": {"w": [300.0]}, "objectives": {"flooding": 2.5},
              "steps": 1}
    definition = {"workflow": "gym.evaluate", "workflow_version": "1",
                  "model_inputs": {"model/model.inp": "b" * 64},
                  "env_config": {"env_type": "cip", "inp_path": "model/model.inp"},
                  "dimensions": [{"label": "field_design:S1", "low": 100.0, "high": 900.0}],
                  "objectives": [{"name": "flooding", "direction": "minimize"}],
                  "optimization": {"algorithm": "alternatives", "budget": 1}, "seed": 7}
    local = StudyManifest(definition=definition, evaluations=[record], result={"best": record},
                          runtime={"executor": "local", "workers": 0})
    remote = StudyManifest(definition=definition, evaluations=[record], result={"best": record},
                           runtime={"executor": "platform", "study_id": "std_1"})
    assert local.comparable() == remote.comparable() and local != remote
    document = local.model_dump(mode="json", by_alias=True)
    assert document["schema"] == "hydrocouple.platform/study-manifest/1"
    jsonschema.validate(document, _schema("study-manifest-1.json"))
    assert document["evaluations"][0]["failed"] is False


def test_the_views_match_what_the_services_return():
    when = datetime(2026, 11, 20, tzinfo=timezone.utc).isoformat()
    ApprovalView.model_validate({"approval_id": "apr_1", "run_id": "run_1", "job_id": "job_1",
                                 "study_id": None, "reasons": ["over budget"],
                                 "expires_at": when})
    session = {"agent_session_id": "ags_1", "expires_at": when, "budget": {"max_runs": 2}}
    AgentSessionView.model_validate(session)
    with pytest.raises(ValidationError):
        AgentSessionView.model_validate(session | {"agent_session_token": "hcps_secret"})
