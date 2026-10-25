"""Test helpers: an organization with a project and a target, and in-process clients."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from conftest import DATA, ISSUER
from hydrocouple_platform.contracts.agent import (
    AttemptOutcome,
    AttemptResult,
    GrantFulfillment,
    Heartbeat,
    LeaseGrant,
    PollRequest,
)
from hydrocouple_platform.contracts.capabilities import AdapterCapabilities
from hydrocouple_platform.contracts.jobs import JobClass
from hydrocouple_platform.contracts.manifests import (
    ArtifactEntry,
    ArtifactKind,
    AttemptManifest,
    Provenance,
    RuntimeVersions,
    canonical_json,
    sha256_of,
)
from hydrocouple_platform.contracts.requests import RevisionCreate, RunRequest, TargetCreate
from hydrocouple_platform.packaging import package_swmm_model
from hydrocouple_platform.services import PlatformServices, VerifiedClaims
from hydrocouple_platform.storage import LocalStore, sha256_file


def local_capabilities(**overrides) -> AdapterCapabilities:
    base = dict(adapter="local-pool", family="local", job_classes=[JobClass.SINGLE],
                has_runtime_cap=False, max_parallel_tasks=1, max_cpu=8, max_memory_gib=16,
                gpu=[], scratch_gib=100, scratch_in_memory=False, interruption_tolerant=False,
                provider_retries=0, idempotent_submit=True, status_integration="direct",
                runs_images=False)
    base.update(overrides)
    return AdapterCapabilities(**base)


class DirectAgentClient:
    """The agent's client, calling the services in process instead of over HTTP."""

    def __init__(self, services: PlatformServices, target_id: str):
        self.services, self.target_id = services, target_id

    def poll(self, req: PollRequest):
        return self.services.agent_poll(self.target_id, req)

    def fulfill(self, body: GrantFulfillment) -> None:
        from hydrocouple_platform.db.models import ComputeTarget

        target = ComputeTarget(id=self.target_id)
        self.services.agent_fulfill_grant(target, body)

    def heartbeat(self, token: str, hb: Heartbeat):
        return self.services.agent_heartbeat(self.target_id, token, hb)

    def result(self, token: str, res: AttemptResult):
        return self.services.agent_complete(self.target_id, token, res)

    def lease_evaluations(self, token: str, req):
        return self.services.evaluation_lease(self.target_id, token, req)

    def complete_evaluations(self, token: str, body):
        return self.services.evaluation_complete(self.target_id, token, body)


@dataclass
class World:
    services: PlatformServices
    org_id: str
    admin: object
    project_id: str
    target_id: str
    credential: str
    store: LocalStore
    store_uri: str
    root: Path

    def principal(self, subject: str = "alice"):
        return self.services.principal_for(VerifiedClaims(ISSUER, subject, None))

    def poll(self, free_slots: int = 1, caps: AdapterCapabilities | None = None, instance="a1",
             held: list[str] | None = None):
        return self.services.agent_poll(self.target_id, PollRequest(
            agent_instance=instance, agent_version="test",
            capabilities=caps or local_capabilities(), free_slots=free_slots,
            held_attempts=held or []))

    def run_request(self, revision_id: str, **kw) -> RunRequest:
        return RunRequest(project_id=self.project_id, revision_id=revision_id,
                          workflow="swmm.simulation", workflow_version="1",
                          target_id=self.target_id, **kw)


def make_world(services: PlatformServices, root: Path, egress: str = "summaries",
               org_name: str = "Utility A", admin: str = "alice") -> World:
    store_root = root / "store"
    store_uri = "file://" + str(store_root.resolve())
    org_id = services.bootstrap_organization(org_name, ISSUER, admin)
    principal = services.principal_for(VerifiedClaims(ISSUER, admin, None))
    project = services.create_project(principal, org_id, "Stormwater master plan")
    target = services.register_target(principal, org_id, TargetCreate(
        name="site-a", ownership_mode="organization-owned", adapter="local-pool",
        storage_uri=store_uri, egress_policy=egress))
    world = World(services, org_id, principal, project["project_id"], target["target_id"],
                  target["agent_credential"], LocalStore(store_root), store_uri, root)
    world.poll(free_slots=0)  # the agent reports its capabilities
    return world


def answer_grants(world: World) -> None:
    """Let the agent answer pending grant requests, as its poll loop would."""
    from hydrocouple_platform.agent import Agent
    from hydrocouple_platform.contracts.jobs import EgressPolicy

    agent = Agent(DirectAgentClient(world.services, world.target_id), adapter=None,  # type: ignore[arg-type]
                  store=world.store, store_uri=world.store_uri, egress=EgressPolicy.SUMMARIES)
    response = world.poll(free_slots=0)
    for request in response.grant_requests:
        agent._answer(request)


def ready_revision(world: World, inp: Path | None = None) -> dict:
    """Package a model, upload it through grants and verify it; returns the revision."""
    inp = inp or DATA / "site_drainage_example.inp"
    package = package_swmm_model(inp, world.root / "package")
    created = world.services.create_revision(world.admin, RevisionCreate(
        project_id=world.project_id, target_id=world.target_id, manifest=package.manifest))
    answer_grants(world)
    grants = world.services.get_grant_request(world.admin, created["grant_request_id"])
    by_key = {g["key"]: g for g in grants["grants"]}
    for path, source in package.sources().items():
        dest = Path(by_key[created["storage_prefix"] + path]["url"][len("file://"):])
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
    world.services.finalize_revision(world.admin, created["revision_id"])
    answer_grants(world)
    return world.services.get_revision(world.admin, created["revision_id"])


def fake_outputs(world: World, lease: LeaseGrant, *, status: str = "completed",
                 drop: str | None = None) -> AttemptResult:
    """Publish plausible outputs for a lease without running the engine."""
    spec = lease.spec
    out = world.root / "fake-outputs" / lease.attempt_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "model.rpt").write_text("report\n")
    (out / "model.out").write_bytes(b"\x00binary\x00")
    run = {"schema_version": "1.0", "run": {"id": spec.run_id, "status": status},
           "components": [{"id": "swmm"}], "results": []}
    (out / "run.json").write_text(json.dumps(run))
    entries = []
    for name, kind, fmt in [("run.json", ArtifactKind.RUN_MANIFEST, "json"),
                            ("model.rpt", ArtifactKind.REPORT, "swmm_rpt"),
                            ("model.out", ArtifactKind.BINARY_OUTPUT, "swmm_out")]:
        if name == drop:
            continue
        world.store.put_file(f"{spec.outputs.storage_prefix}{lease.attempt_id}/{name}", out / name)
        entries.append(ArtifactEntry(path=name, sha256=sha256_file(out / name),
                                     size=(out / name).stat().st_size, kind=kind, format=fmt))
    now = datetime.now(timezone.utc)
    manifest = AttemptManifest(
        run_manifest_sha256=sha256_file(out / "run.json"), status=status, artifacts=entries,
        quality={"continuity_errors": {"runoff": 0.0, "routing": 0.0}},
        provenance=Provenance(
            run_id=spec.run_id, job_id=spec.job_id, attempt_id=lease.attempt_id,
            attempt_number=lease.fencing_token, revision_id=spec.inputs.revision_id,
            revision_manifest_sha256=spec.inputs.manifest_sha256, composition_sha256="0" * 64,
            inputs={f.path: f.sha256 for f in spec.inputs.files}, workflow="swmm.simulation",
            workflow_version="1", parameters=spec.workload.parameters, threads=1,
            runtime=RuntimeVersions(platform="test", python="3"), actor=spec.actor,
            started_at=now, finished_at=now),
    ).model_dump(mode="json", by_alias=True)
    (out / "attempt.json").write_bytes(canonical_json(manifest))
    world.store.put_file(f"{spec.outputs.storage_prefix}{lease.attempt_id}/attempt.json",
                         out / "attempt.json")
    return AttemptResult(attempt_id=lease.attempt_id, fencing_token=lease.fencing_token,
                         outcome=AttemptOutcome.SUCCEEDED, exit_code=0,
                         attempt_manifest=manifest, attempt_manifest_sha256=sha256_of(manifest))
