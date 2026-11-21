"""Compositions of OpenSWMM engine components (Phase 5, work packages 5.1b and 5.1c).

An authored composition packages with its component instances and the models they run; the
control plane checks the instances against the files; ``composition.run@1`` is refused on a
target without a composition host. With a host (``HCP_TEST_COMPOSER``, the HydroCoupleComposer
executable, and ``HCP_TEST_COMPONENT_PATH``, the directories holding the engine component
module), both split examples run through the platform: one run manifest and one attempt
manifest name both components, the material split's attempt marks it experimental, each
component's outputs are published and equal a direct run of the host on the same files, byte
for byte; a component the engine refuses fails the attempt with the host's message. Files are
kept under ``test_artifacts/``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest

from conftest import DATA
from hydrocouple_platform.agent import Agent, AgentClient
from hydrocouple_platform.agent.adapters.local import LocalPoolAdapter
from hydrocouple_platform.agent.config import CompositionHostConfig
from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.errors import PlatformError
from hydrocouple_platform.contracts.jobs import EgressPolicy
from hydrocouple_platform.contracts.requests import RevisionCreate
from hydrocouple_platform.packaging import package_composition
from hydrocouple_platform.packaging.package import PackagingError
from hydrocouple_platform.storage import sha256_file
from world import make_world

COMPOSITIONS = DATA / "compositions"
HOST = os.environ.get("HCP_TEST_COMPOSER")
COMPONENT_PATHS = [p for p in os.environ.get("HCP_TEST_COMPONENT_PATH", "").split(os.pathsep)
                   if p]


def copy(name: str, dest: Path) -> Path:
    shutil.copytree(COMPOSITIONS / name, dest)
    return dest / "composition.json"


def test_an_authored_composition_packages_with_its_component_instances(artifact_dir):
    for name, roles in (("split-water", ["integrated2d", "split_network"]),
                        ("split-material", ["split_material_surface", "split_material_network"])):
        package = package_composition(COMPOSITIONS / name / "composition.json",
                                      artifact_dir / name)
        m = package.manifest
        assert [(f.path, f.role.value) for f in m.files] == [("composition.json", "composition"),
                                                            ("model/model.inp", "model")]
        assert [(i.id, i.role, i.model) for i in m.component_instances] == [
            ("surface", roles[0], "model/model.inp"), ("network", roles[1], "model/model.inp")]
        packaged = json.loads((artifact_dir / name / "composition.json").read_text())
        assert {c["arguments"]["configuration"]["model"] for c in packaged["components"]} == {
            "model/model.inp"}
        assert packaged["connections"] == json.loads(
            (COMPOSITIONS / name / "composition.json").read_text())["connections"]


def test_a_composition_that_cannot_be_packaged_is_refused_with_every_reason(artifact_dir):
    source = copy("split-water", artifact_dir / "source")
    doc = json.loads(source.read_text())
    doc["components"][0]["info"]["component_info_id"] = "org.example.other"
    doc["components"][1]["arguments"]["configuration"]["model"] = "../outside.inp"
    source.write_text(json.dumps(doc))
    with pytest.raises(PackagingError) as refused:
        package_composition(source, artifact_dir / "refused")
    assert any("only the OpenSWMM engine component" in p for p in refused.value.problems)
    assert any("'..'" in p or "escape" in p for p in refused.value.problems)
    source.write_text(json.dumps({"components": "not a list"}))
    with pytest.raises(PackagingError, match="is not a HydroCouple composition"):
        package_composition(source, artifact_dir / "invalid")


@pytest.mark.postgres
def test_the_control_plane_checks_the_instances_and_wants_a_host(services, artifact_dir):
    world = make_world(services, artifact_dir)
    package = package_composition(COMPOSITIONS / "split-water" / "composition.json",
                                  artifact_dir / "package")
    for change, message in (
            (lambda m: m.component_instances[1].__setattr__("model", "model/other.inp"),
             "does not list as a model"),
            (lambda m: m.component_instances[1].__setattr__("id", "surface"), "distinct ids"),
            (lambda m: m.__setattr__("components", ["org.example.other"]),
             "name different components")):
        manifest = package.manifest.model_copy(deep=True)
        change(manifest)
        with pytest.raises(PlatformError, match=message):
            services.create_revision(world.admin, RevisionCreate(
                project_id=world.project_id, target_id=world.target_id, manifest=manifest))
    from world import answer_grants

    created = services.create_revision(world.admin, RevisionCreate(
        project_id=world.project_id, target_id=world.target_id, manifest=package.manifest))
    answer_grants(world)
    grants = services.get_grant_request(world.admin, created["grant_request_id"])
    for g in grants["grants"]:
        dest = Path(g["url"][len("file://"):])
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(package.sources()[g["key"][len(created["storage_prefix"]):]], dest)
    services.finalize_revision(world.admin, created["revision_id"])
    answer_grants(world)
    estimate = services.estimate(world.admin, world.run_request(created["revision_id"])
                                 .model_copy(update={"workflow": "composition.run"}))
    assert not estimate.ok
    assert any("has no composition host hydrocouple.composer" in f for f in estimate.findings)


class Platform:
    """The dispatcher, reconciler and agent loops, in the background, with the host."""

    def __init__(self, services, world, server, artifact_dir):
        self.services = services
        self.adapter = LocalPoolAdapter(
            artifact_dir / "agent-work", slots=2, keep_workdirs=True, grace_seconds=3,
            composition_host=CompositionHostConfig(executable=HOST,
                                                   component_paths=COMPONENT_PATHS))
        self.agent = Agent(AgentClient(server, world.credential), self.adapter, world.store,
                           world.store_uri, EgressPolicy.SUMMARIES)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.errors: list[Exception] = []

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.services.dispatch()
                self.services.reconcile()
                self.agent.run_once()
            except Exception as exc:
                self.errors.append(exc)
                return
            time.sleep(0.1)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=30)
        for handle in self.adapter.handles():
            self.adapter.cancel(handle)
        self.agent.close()
        if self.errors:
            raise self.errors[0]


def direct_run(source: Path, folder: Path) -> Path:
    """The host run directly on the same files, outside the platform."""
    shutil.copytree(source.parent, folder)
    command = [HOST, "--run", str(folder / "composition.json")]
    for p in COMPONENT_PATHS:
        command += ["--components", p]
    done = subprocess.run(command, cwd=folder, capture_output=True, text=True, timeout=600,
                          env={**os.environ, "QT_QPA_PLATFORM": "offscreen"})
    (folder / "composer.log").write_text(done.stdout + done.stderr)
    assert done.returncode == 0, done.stdout[-2000:]
    return folder


@pytest.mark.postgres
@pytest.mark.engine
@pytest.mark.skipif(not (HOST and COMPONENT_PATHS),
                    reason="set HCP_TEST_COMPOSER and HCP_TEST_COMPONENT_PATH to a composition "
                           "host (HydroCoupleComposer and the engine component module)")
def test_both_split_examples_run_through_the_platform_as_one_run(services, artifact_dir, server,
                                                                 make_token):
    world = make_world(services, artifact_dir)
    broken = copy("split-water", artifact_dir / "broken")
    doc = json.loads(broken.read_text())
    doc["components"][0]["arguments"]["configuration"]["surface_results"] = "yes"
    broken.write_text(json.dumps(doc, indent=2))
    runs = {}
    with Platform(services, world, server, artifact_dir), \
            PlatformClient(server, make_token("alice")) as client:
        for name, source in (("split-water", COMPOSITIONS / "split-water" / "composition.json"),
                             ("split-material",
                              COMPOSITIONS / "split-material" / "composition.json"),
                             ("broken", broken)):
            revision = client.upload_revision(
                package_composition(source, artifact_dir / "package" / name),
                project_id=world.project_id, target_id=world.target_id)
            accepted = client.submit_run({
                "project_id": world.project_id, "revision_id": revision["revision_id"],
                "workflow": "composition.run", "workflow_version": "1",
                "target_id": world.target_id, "retry_budget": 0},
                idempotency_key=f"composition-{name}")
            runs[name] = (accepted, client.wait_for_job(accepted["job_id"], timeout=600))
        downloads = {}
        for name in ("split-water", "split-material"):
            accepted, view = runs[name]
            assert view["state"] == "SUCCEEDED", view
            results = client.results(accepted["run_id"])
            paths = [a["path"] for a in results["artifacts"]
                     if a["path"].startswith("components/") and a["path"].endswith(
                         ("model.out", "model.2d.h5"))]
            downloads[name] = (client.download(accepted["run_id"], ["run.json", *paths],
                                               artifact_dir / "downloaded" / name),
                               client.attempt_manifest(accepted["run_id"])["attempt_manifest"])

    for name, roles, qualification in (
            ("split-water", ("integrated2d", "split_network"), "qualified"),
            ("split-material", ("split_material_surface", "split_material_network"),
             "experimental")):
        files, manifest = downloads[name]
        run_manifest = json.loads(files["run.json"].read_text())
        assert [(c["id"], c["final_status"]) for c in run_manifest["components"]] == [
            ("surface", "Finished"), ("network", "Finished")]
        assert [(c["id"], c["role"], c["final_status"], c["qualification"])
                for c in manifest["provenance"]["components"]] == [
            ("surface", roles[0], "Finished", qualification),
            ("network", roles[1], "Finished", qualification)]
        direct = direct_run(COMPOSITIONS / name / "composition.json",
                            artifact_dir / "direct" / name)
        outputs = {"network": "network-results/model.out",
                   "surface": "surface-results/model.2d.h5"}
        for component, rel in outputs.items():
            published = files[f"components/{component}/{Path(rel).name}"]
            assert sha256_file(published) == sha256_file(direct / rel), (name, component)
    broken_view = runs["broken"][1]
    assert broken_view["state"] == "FAILED" and broken_view["failure"]["exit_code"] == 12
    assert "the composition failed" in broken_view["failure"]["diagnostics"]["message"]
