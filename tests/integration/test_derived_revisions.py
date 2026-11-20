"""A derived revision must name a base revision, by its manifest hash, and a study it may see."""

from __future__ import annotations

import pytest

from conftest import DATA
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.manifests import DerivedFrom
from hydrocouple_platform.contracts.requests import RevisionCreate
from hydrocouple_platform.packaging import package_swmm_model
from world import make_world, ready_revision

pytestmark = pytest.mark.postgres


def _create(world, lineage: DerivedFrom) -> dict:
    package = package_swmm_model(DATA / "site_drainage_example.inp", world.root / "derived")
    manifest = package.manifest.model_copy(update={"derived_from": lineage})
    return world.services.create_revision(world.admin, RevisionCreate(
        project_id=world.project_id, target_id=world.target_id, manifest=manifest))


def test_the_base_must_exist_with_its_hash_and_the_study_must_have_evaluated_it(
        services, artifact_dir):
    world = make_world(services, artifact_dir)
    base = ready_revision(world)
    good = _create(world, DerivedFrom(revision_id=base["revision_id"],
                                      revision_manifest_sha256=base["manifest_sha256"]))
    stored = world.services.get_revision(world.admin, good["revision_id"])
    assert stored["manifest"]["derived_from"]["revision_id"] == base["revision_id"]
    assert "derived_from" not in base["manifest"]

    cases = [
        (DerivedFrom(revision_id="rev_nowhere", revision_manifest_sha256="0" * 64),
         ErrorCode.NOT_FOUND),
        (DerivedFrom(revision_id=base["revision_id"], revision_manifest_sha256="0" * 64),
         ErrorCode.VALIDATION_ERROR),
        (DerivedFrom(revision_id=base["revision_id"],
                     revision_manifest_sha256=base["manifest_sha256"], study_id="std_nowhere"),
         ErrorCode.NOT_FOUND),
    ]
    for lineage, code in cases:
        with pytest.raises(PlatformError) as err:
            _create(world, lineage)
        assert err.value.code == code, lineage
