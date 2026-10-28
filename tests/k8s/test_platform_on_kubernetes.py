"""Real workflows on a real Kubernetes cluster, run from signed images.

The control plane runs in this process. The agent drives the Kubernetes adapter, and
each attempt is a Job running the runner image. The agent's ``file://`` store is a
PersistentVolumeClaim, so the agent (on the node) and the runner pods share it, as an
NFS claim would on premises.

Besides the cluster (``HCP_TEST_KUBECONFIG``) these tests need:
- ``HCP_TEST_K8S_RUNNER_IMAGE``: the runner image by digest, signed with the key below
  (``images/build-runner.sh``);
- ``HCP_TEST_K8S_IMAGE_KEY``: that key's cosign public key;
- ``HCP_TEST_K8S_CONTRACT_IMAGE``: the stand-in runner, signed with another key;
- ``HCP_TEST_K8S_UNSIGNED_IMAGE`` (optional): an image with no signature at all.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import threading
import time
from pathlib import Path

import pytest

from conftest import DATA
from hydrocouple_platform.agent import Agent
from hydrocouple_platform.agent.adapters.kubernetes import KubernetesAdapter, KubernetesConfig
from hydrocouple_platform.agent.imagetrust import ImageTrust, ImageVerifier
from hydrocouple_platform.contracts.jobs import EgressPolicy
from world import DirectAgentClient, make_world, ready_revision

pytestmark = [pytest.mark.k8s, pytest.mark.postgres, pytest.mark.engine]
CLAIM = "hc-store"
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED"}


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"set {name} to run the platform on Kubernetes")
    return value


@pytest.fixture(scope="module")
def runner_image():
    return _env("HCP_TEST_K8S_RUNNER_IMAGE")


@pytest.fixture(scope="module")
def image_key():
    return _env("HCP_TEST_K8S_IMAGE_KEY")


@pytest.fixture(scope="module")
def shared_store(kube, namespace, contract_image):
    """A claim in the test namespace, and its directory on the (single) node."""
    from kubernetes import client as k8s

    core = k8s.CoreV1Api(kube)
    core.create_namespaced_persistent_volume_claim(namespace, {
        "metadata": {"name": CLAIM},
        "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "2Gi"}}}})
    # The claim binds when a pod first uses it.
    core.create_namespaced_pod(namespace, {
        "metadata": {"name": "bind-store"},
        "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                 "securityContext": {"runAsNonRoot": True, "runAsUser": 10001,
                                     "seccompProfile": {"type": "RuntimeDefault"}},
                 "containers": [{"name": "bind", "image": contract_image,
                                 "command": ["python"],
                                 "securityContext": {"allowPrivilegeEscalation": False,
                                                     "capabilities": {"drop": ["ALL"]}},
                                 "volumeMounts": [{"name": "store", "mountPath": "/store"}]}],
                 "volumes": [{"name": "store",
                              "persistentVolumeClaim": {"claimName": CLAIM}}]}})
    deadline = time.monotonic() + 120
    while core.read_namespaced_persistent_volume_claim(CLAIM, namespace).status.phase != "Bound":
        assert time.monotonic() < deadline, "the store claim did not bind"
        time.sleep(1)
    volume = core.read_persistent_volume(
        core.read_namespaced_persistent_volume_claim(CLAIM, namespace).spec.volume_name)
    path = (volume.spec.host_path or volume.spec.local).path
    core.delete_namespaced_pod("bind-store", namespace)
    return Path(path)


@pytest.fixture
def world(services, artifact_dir, shared_store):
    # The world's store is the claim's directory, as the agent on the node sees it.
    (artifact_dir / "store").symlink_to(shared_store, target_is_directory=True)
    return make_world(services, artifact_dir)


def node_address(kube) -> str:
    from kubernetes import client as k8s

    (node,) = k8s.CoreV1Api(kube).list_node().items
    return next(a.address for a in node.status.addresses if a.type == "InternalIP")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("0.0.0.0", 0))
        return s.getsockname()[1]


def kube_agent(world, kube, namespace, image_key, **kw) -> Agent:
    cfg = KubernetesConfig(namespace=namespace, max_cpu=2, max_memory_gib=4, scratch_gib=8,
                           store_volume={"mount_path": "/store", "claim_name": CLAIM},
                           poll_seconds=1, grace_seconds=10, start_grace_seconds=60)
    trust = ImageVerifier(ImageTrust(keys=[image_key], allow_http_registry=True))
    return Agent(DirectAgentClient(world.services, world.target_id),
                 KubernetesAdapter(cfg, api_client=kube), world.store, world.store_uri,
                 EgressPolicy.SUMMARIES, images=trust, **kw)


def drive(world, agent, done, timeout=600):
    deadline = time.monotonic() + timeout
    while not done():
        assert time.monotonic() < deadline, "the work did not finish in time"
        world.services.dispatch()
        world.services.reconcile()
        agent.run_once()
        time.sleep(0.5)


def submit(world, revision, key):
    accepted = world.services.submit_run(world.admin, world.run_request(revision["revision_id"]),
                                         key)
    world.services.dispatch()
    return accepted


def env_config(inp_path: str) -> dict:
    """The twin-tank design study of tests/integration/test_studies_engine.py."""
    return {
        "env_type": "cip", "inp_path": inp_path,
        "design_factories": [
            {"kind": "node_max_depth",
             "params": {"node_ids": ["T1", "T2"], "low": 2.0, "high": 4.0}},
            {"kind": "link_diameter",
             "params": {"link_ids": ["OUT"], "low": 0.5, "high": 3.0}}],
        "observations": {"node_depths": ["T1", "T2"]},
        "reward_terms": [{"kind": "flooding_volume", "params": {}},
                         {"kind": "peak_outflow", "params": {"link_ids": ["OUT"]}}],
    }


def jobs_for_run(kube, namespace, run_id):
    from kubernetes import client as k8s

    return k8s.BatchV1Api(kube).list_namespaced_job(
        namespace, label_selector=f"hydrocouple.org/run={run_id}").items


def test_a_swmm_run_on_kubernetes_publishes_verified_results(
        world, kube, namespace, runner_image, image_key):
    svc = world.services
    svc.settings.workflow_images = {"swmm.simulation@1": runner_image}
    agent = kube_agent(world, kube, namespace, image_key)
    agent.run_once()  # the agent reports the Kubernetes capabilities
    revision = ready_revision(world)
    accepted = submit(world, revision, "k8s-run-0001")
    drive(world, agent, lambda: svc.get_job(world.admin, accepted.job_id).state in TERMINAL)
    job = svc.get_job(world.admin, accepted.job_id)
    assert job.state == "SUCCEEDED", job.failure
    results = svc.get_results(world.admin, accepted.run_id)
    assert {a["path"] for a in results["artifacts"]} >= {"run.json", "model.out", "model.rpt"}
    runtime = results["provenance"]["runtime"]
    assert runtime["image"] == runner_image  # the provenance names the signed digest
    assert abs(results["quality"]["continuity_errors"]["routing"]) < 5
    (world.root / "kubernetes-results.json").write_text(json.dumps(results, indent=2))
    deadline = time.monotonic() + 60
    while jobs_for_run(kube, namespace, accepted.run_id):  # cleaned up once reported
        assert time.monotonic() < deadline
        time.sleep(1)
    agent.close()


@pytest.mark.parametrize("which", ["signed-by-another-key", "unsigned"])
def test_images_no_pinned_key_signed_are_refused_and_never_retried(
        world, kube, namespace, image_key, contract_image, which):
    image = contract_image if which == "signed-by-another-key" else \
        os.environ.get("HCP_TEST_K8S_UNSIGNED_IMAGE")
    if not image:
        pytest.skip("set HCP_TEST_K8S_UNSIGNED_IMAGE to test an unsigned image")
    svc = world.services
    svc.settings.workflow_images = {"swmm.simulation@1": image}
    agent = kube_agent(world, kube, namespace, image_key)
    agent.run_once()
    accepted = submit(world, ready_revision(world), f"k8s-refused-{which}")
    drive(world, agent, lambda: svc.get_job(world.admin, accepted.job_id).state in TERMINAL,
          timeout=180)
    job = svc.get_job(world.admin, accepted.job_id)
    assert job.state == "FAILED" and job.failure["class"] == "untrusted_image"
    assert "not signed by a pinned key" in job.failure["diagnostics"]["message"]
    assert len(job.attempts) == 1  # never retried
    assert jobs_for_run(kube, namespace, accepted.run_id) == []  # nothing ever started
    agent.close()


def test_a_study_on_a_kubernetes_pool_matches_a_local_run(
        world, kube, namespace, runner_image, image_key, server, make_token, artifact_dir):
    pytest.importorskip("openswmm_gymnasium.optimize")
    from openswmm_gymnasium.optimize import OptimizationConfig, run_study
    from openswmm_gymnasium.spec.config import EnvConfig

    from hydrocouple_platform.client import PlatformClient
    from hydrocouple_platform.gym import run_platform_study

    svc = world.services
    svc.settings.workflow_images = {"gym.evaluate@1": runner_image}
    port = free_port()
    agent = kube_agent(world, kube, namespace, image_key, gateway_host="0.0.0.0",
                       gateway_port=port, gateway_url=f"http://{node_address(kube)}:{port}")
    stop, errors = threading.Event(), []

    def loop():
        while not stop.is_set():
            try:
                svc.dispatch()
                svc.reconcile()
                agent.run_once()
            except Exception as exc:  # surfaced after the study
                errors.append(exc)
                return
            time.sleep(0.3)

    model = DATA / "twin_tank_heavy" / "model.inp"
    revision = ready_revision(world, model)
    opt = OptimizationConfig(algorithm="random_search", budget=24, seed=3, workers=0,
                             on_failure="penalty")
    local_model = artifact_dir / "local-model" / "model.inp"
    local_model.parent.mkdir()
    shutil.copyfile(model, local_model)
    reference = run_study(EnvConfig.model_validate(env_config(str(local_model))), opt,
                          artifact_dir / "local")
    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    try:
        with PlatformClient(server, make_token("alice")) as client:
            result = run_platform_study(
                EnvConfig.model_validate(env_config("model/model.inp")), opt,
                artifact_dir / "platform", client=client, project_id=world.project_id,
                revision_id=revision["revision_id"], target_id=world.target_id,
                inp_path="model/model.inp", pool_size=1, batch_size=12)
    finally:
        stop.set()
        thread.join(timeout=30)
        agent.close()
    assert not errors, errors
    local = [json.loads(x) for x in (artifact_dir / "local" / "evaluations.jsonl").open()]
    remote = [json.loads(x) for x in (artifact_dir / "platform" / "evaluations.jsonl").open()]
    assert [r["decisions"] for r in remote] == [r["decisions"] for r in local]
    for a, b in zip(local, remote, strict=True):
        assert b["objectives"] == pytest.approx(a["objectives"], rel=1e-12, abs=1e-9)
    assert [p["evaluation"] for p in result["pareto"]] == \
        [p["evaluation"] for p in reference["pareto"]]
