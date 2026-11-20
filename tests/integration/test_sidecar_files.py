"""Observed data and other files a model does not reference travel in its revision (plan 3.2)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from conftest import DATA
from hydrocouple_platform.contracts.manifests import FileRole
from hydrocouple_platform.contracts.requests import RevisionCreate
from hydrocouple_platform.packaging import package_swmm_model
from hydrocouple_platform.packaging.package import PackagingError
from world import answer_grants, make_world

pytestmark = pytest.mark.postgres


def observed(folder: Path) -> Path:
    path = folder / "observed.csv"
    path.write_text("time,flow\n1998-01-01T00:15:00,0.5\n1998-01-01T00:30:00,4.2\n")
    return path


def upload(world, package, tamper: Path | None = None) -> dict:
    """Upload a package through grants, optionally swapping one file's bytes, and verify it."""
    created = world.services.create_revision(world.admin, RevisionCreate(
        project_id=world.project_id, target_id=world.target_id, manifest=package.manifest))
    answer_grants(world)
    grants = world.services.get_grant_request(world.admin, created["grant_request_id"])
    by_key = {g["key"]: g for g in grants["grants"]}
    for path, source in package.sources().items():
        dest = Path(by_key[created["storage_prefix"] + path]["url"][len("file://"):])
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tamper if tamper is not None and path.endswith(".csv") else source, dest)
    world.services.finalize_revision(world.admin, created["revision_id"])
    answer_grants(world)
    return world.services.get_revision(world.admin, created["revision_id"])


def test_a_sidecar_is_hashed_and_verified_like_every_input(services, artifact_dir):
    world = make_world(services, artifact_dir)
    obs = observed(artifact_dir)
    package = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "package",
                                 extra_files={"data/observed.csv": obs})
    (entry,) = [f for f in package.manifest.files if f.role == FileRole.SIDECAR]
    assert (entry.path, entry.source) == ("model/data/observed.csv", str(obs))
    assert upload(world, package)["state"] == "READY"

    changed = artifact_dir / "changed.csv"
    changed.write_text(obs.read_text().replace("4.2", "9.9"))
    again = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "package2",
                               extra_files={"data/observed.csv": obs})
    assert upload(world, again, tamper=changed)["state"] == "FAILED"


def test_a_sidecar_cannot_replace_or_escape_the_model(artifact_dir):
    obs = observed(artifact_dir)
    model = DATA / "external_rain" / "model.inp"
    for rel, reason in (("model.inp", "would replace"), ("rain/2-yr storm.dat", "would replace"),
                        ("../escape.csv", "extra file '../escape.csv'"),
                        ("data/missing.csv", "does not exist")):
        source = artifact_dir / "nowhere.csv" if reason == "does not exist" else obs
        with pytest.raises(PackagingError, match=reason):
            package_swmm_model(model, artifact_dir / "refused", extra_files={rel: source})
