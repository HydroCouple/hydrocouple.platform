"""The agent installed in a cluster from ``deploy/kustomize``, as a site would install it.

One installation serves the module:
- **Control plane:** runs in this process, over HTTP on an address pods can reach.
- **Store:** S3 (moto, on the node).
- **Agent:** runs in the cluster from its signed image, installed by an overlay this
  module writes over ``deploy/kustomize/base``. Its credential and the store's
  credentials are Secrets.

The tests check the installation the way onboarding does:
- ``hcp agent --verify`` from inside the agent pod passes, including a runner pod that
  writes to the store;
- the runner network policy lets runner pods reach DNS, the agent's gateway and the
  store, and nothing else.

Besides the cluster (``HCP_TEST_KUBECONFIG``) this needs:
- ``HCP_TEST_K8S_AGENT_IMAGE``, ``HCP_TEST_K8S_RUNNER_IMAGE`` and ``HCP_TEST_K8S_IMAGE_KEY``:
  both images by digest, and the cosign public key that signed them;
- ``HCP_TEST_K8S_POD_REGISTRY``: the registry's address as pods reach it (the agent pulls
  through it as its mirror), when the image references name an address only the node
  can reach, such as ``127.0.0.1:5000``;
- ``HCP_TEST_KUBECTL``: the kubectl command (default ``kubectl``; ``k3s kubectl`` on k3s).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest

from conftest import ARTIFACTS, AUDIENCE, ISSUER, ROOT
from emulators import free_port, moto

pytestmark = [pytest.mark.k8s, pytest.mark.postgres]
BASE = ROOT / "deploy" / "kustomize" / "base"
CREDENTIALS = {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
               "AWS_DEFAULT_REGION": "us-east-1"}


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"set {name} to test the agent installed in a cluster")
    return value


def kubectl(*args: str, input: str | None = None, check: bool = True) -> str:
    command = [*os.environ.get("HCP_TEST_KUBECTL", "kubectl").split(), *args]
    env = {**os.environ, "KUBECONFIG": os.environ["HCP_TEST_KUBECONFIG"]}
    done = subprocess.run(command, input=input, capture_output=True, text=True, env=env,
                          timeout=300)
    if check and done.returncode != 0:
        raise AssertionError(f"{' '.join(command)} failed: {done.stderr or done.stdout}")
    return done.stdout


def node_address(kube) -> str:
    from kubernetes import client as k8s

    (node,) = k8s.CoreV1Api(kube).list_node().items
    return next(a.address for a in node.status.addresses if a.type == "InternalIP")


def write_overlay(path: Path, namespace: str, agent_image: str, config: dict, key: Path,
                  node: str, store_port: int) -> None:
    """An overlay like deploy/kustomize/overlays/onprem, with this cluster's values."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "agent.json").write_text(json.dumps(config, indent=2))
    shutil.copyfile(key, path / "release.pub")
    repository, _, digest = agent_image.partition("@")
    (path / "kustomization.yaml").write_text(f"""\
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: {namespace}
resources:
  - {os.path.relpath(BASE, path)}
images:
  - name: hydrocouple/agent
    newName: {repository}
    digest: {digest}
configMapGenerator:
  - name: hcp-agent-config
    files: [agent.json]
  - name: hcp-image-keys
    files: [release.pub]
patches:
  - target: {{kind: NetworkPolicy, name: hcp-runner-storage}}
    patch: |-
      - op: replace
        path: /spec/egress/0
        value:
          to: [{{ipBlock: {{cidr: {node}/32}}}}]
          ports: [{{protocol: TCP, port: {store_port}}}]
  - target: {{kind: Deployment, name: hcp-agent}}
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/envFrom
        value: [{{secretRef: {{name: hcp-store-credentials}}}}]
""")


class ControlPlane:
    """The control plane for the module: services, the HTTP API on all addresses, and the
    dispatcher and reconciler in a thread."""

    def __init__(self, database_url: str, jwks_file: Path, workflow_images: dict):
        import uvicorn

        from hydrocouple_platform.api import create_app
        from hydrocouple_platform.config import ControlPlaneSettings
        from hydrocouple_platform.db import make_engine, make_sessionmaker
        from hydrocouple_platform.services import PlatformServices

        self.settings = ControlPlaneSettings(
            database_url=database_url, jwt_issuer=ISSUER, jwt_audience=AUDIENCE,
            jwks_file=str(jwks_file), attempt_token_secret="s" * 48, lease_seconds=30,
            abandon_after_seconds=120, heartbeat_interval_seconds=1.0,
            workflow_images=workflow_images)
        self.engine = make_engine(database_url)
        self.services = PlatformServices(make_sessionmaker(self.engine), self.settings)
        self.port = free_port("0.0.0.0")
        app = create_app(self.settings, services=self.services)
        self.server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=self.port,
                                                    log_level="warning"))
        self._stop = threading.Event()
        self.errors: list[Exception] = []
        self._threads = [threading.Thread(target=self.server.run, daemon=True),
                         threading.Thread(target=self._work, daemon=True)]
        for thread in self._threads:
            thread.start()
        deadline = time.monotonic() + 15
        while not self.server.started:
            assert time.monotonic() < deadline, "the control plane did not start"
            time.sleep(0.05)

    def _work(self):
        while not self._stop.is_set():
            try:
                self.services.dispatch()
                self.services.reconcile()
            except Exception as exc:  # reported by the tests
                self.errors.append(exc)
            self._stop.wait(0.5)

    def close(self):
        self._stop.set()
        self.server.should_exit = True
        for thread in self._threads:
            thread.join(timeout=10)
        self.engine.dispose()


@pytest.fixture(scope="module")
def install(kube, pg_template, jwks_file):
    """Everything a site has after onboarding, ending with the agent's pod ready."""
    import boto3
    from sqlalchemy import text

    from world import make_world

    agent_image = _env("HCP_TEST_K8S_AGENT_IMAGE")
    runner_image = _env("HCP_TEST_K8S_RUNNER_IMAGE")
    key = Path(_env("HCP_TEST_K8S_IMAGE_KEY"))
    mirror = os.environ.get("HCP_TEST_K8S_POD_REGISTRY")
    root = ARTIFACTS / "test_install_on_kubernetes"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)

    cluster, admin = pg_template
    database = "t_" + uuid.uuid4().hex[:12]
    with admin.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {database} TEMPLATE hcp_template"))
    plane = ControlPlane(cluster.url.replace("/postgres?", f"/{database}?"), jwks_file,
                         {"swmm.simulation@1": runner_image, "gym.evaluate@1": runner_image})
    store, store_port = moto(host="0.0.0.0")
    node = node_address(kube)
    environment = pytest.MonkeyPatch()
    for k, v in CREDENTIALS.items():
        environment.setenv(k, v)
    boto3.client("s3", endpoint_url=f"http://{node}:{store_port}").create_bucket(
        Bucket="hcp-site")
    store_uri = f"s3://hcp-site/platform?endpoint_url=http://{node}:{store_port}&region=us-east-1"
    world = make_world(plane.services, root / "world")
    namespace = "hci-" + uuid.uuid4().hex[:6]
    config = {
        "control_plane": f"http://{node}:{plane.port}", "store": store_uri,
        "egress": "summaries", "adapter": "kubernetes",
        "kubernetes": {"namespace": namespace, "max_cpu": 2, "max_memory_gib": 4,
                       "scratch_gib": 8, "slots": 4, "poll_seconds": 1, "grace_seconds": 10,
                       "runner_env_secret": "hcp-store-credentials"},
        "image_trust": {"keys": ["/etc/hydrocouple/keys/release.pub"], "mirror": mirror,
                        "allow_http_registry": True},
        "gateway": {"host": "0.0.0.0", "port": 8081,
                    "url": f"http://hcp-agent-gateway.{namespace}.svc:8081"},
    }
    overlay = root / "overlay"
    write_overlay(overlay, namespace, agent_image, config, key, node, store_port)
    kubectl("apply", "-k", str(overlay))
    for name, data in (("hcp-agent-credential", {"credential": world.credential}),
                       ("hcp-store-credentials", CREDENTIALS)):
        kubectl("apply", "-f", "-", input=json.dumps({
            "apiVersion": "v1", "kind": "Secret", "metadata": {"name": name,
                                                               "namespace": namespace},
            "stringData": data}))
    kubectl("-n", namespace, "rollout", "status", "deploy/hcp-agent", "--timeout=180s")
    yield {"plane": plane, "world": world, "namespace": namespace, "node": node,
           "store_port": store_port, "store_uri": store_uri, "runner_image": runner_image,
           "mirror": mirror, "root": root, "config": config}
    (root / "agent.log").write_text(kubectl("-n", namespace, "logs", "deploy/hcp-agent",
                                            "--tail=-1", check=False))
    kubectl("delete", "namespace", namespace, "--wait=false", check=False)
    store.kill()
    plane.close()
    environment.undo()


def pod_image(install, image: str) -> str:
    """An image reference as pods pull it (through the mirror, when one is set)."""
    from hydrocouple_platform.agent.imagetrust import mirrored

    return mirrored(image, install["mirror"])


def test_onboarding_verification_passes_from_inside_the_agent_pod(install):
    out = kubectl("-n", install["namespace"], "exec", "deploy/hcp-agent", "--", "hcp", "agent",
                  "--config", "/etc/hydrocouple/config/agent.json", "--verify",
                  "--image", install["runner_image"], check=False)
    (install["root"] / "verify.json").write_text(out)
    result = json.loads(out)
    assert {c["name"]: c["ok"] for c in result["checks"]} == {
        "control plane": True, "scheduler permissions": True, "storage": True,
        "image signature": True, "runner pod": True}, out
    assert result["ok"]


def test_the_agent_reports_kubernetes_capabilities_to_the_control_plane(install):
    from hydrocouple_platform.db.models import ComputeTarget

    plane, world = install["plane"], install["world"]
    deadline = time.monotonic() + 60
    while True:
        with plane.services.tx() as s:
            caps = s.get(ComputeTarget, world.target_id).capabilities
        if caps and caps.get("family") == "kubernetes":
            break
        assert time.monotonic() < deadline, f"the agent never polled: {caps}"
        time.sleep(1)
    assert caps["runs_images"] and caps["provider_retries"] == 0


# Some CNIs (k3s's kube-router among them) program a pod's policy a few seconds after the
# pod starts. The probe waits until the control plane, which policy blocks, is unreachable,
# and then measures every target.
NETWORK_PROBE = """
import json, socket, sys, time
def reach(host, port):
    try:
        socket.create_connection((host, port), timeout=4).close()
        return "open"
    except OSError as exc:
        return type(exc).__name__
targets = json.loads(sys.argv[1])
blocked = next(t for t in targets if t[0] == "control plane")
start = time.monotonic()
while reach(*blocked[1:]) == "open" and time.monotonic() - start < 60:
    time.sleep(0.5)
results = {name: reach(host, port) for name, host, port in targets}
results["seconds until enforced"] = round(time.monotonic() - start, 1)
print(json.dumps(results))
"""


def test_runner_pods_reach_only_dns_the_gateway_and_the_store(install):
    node, ns = install["node"], install["namespace"]
    targets = [["gateway", f"hcp-agent-gateway.{ns}.svc", 8081],
               ["store", node, install["store_port"]],
               ["control plane", node, install["plane"].port],
               ["internet", "1.1.1.1", 443],
               ["kubernetes api", "kubernetes.default.svc", 443]]
    pod = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "network-probe", "namespace": ns,
                     "labels": {"hydrocouple.org/role": "runner"}},
        "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                 "serviceAccountName": "hcp-runner",
                 "securityContext": {"runAsNonRoot": True, "runAsUser": 10001,
                                     "seccompProfile": {"type": "RuntimeDefault"}},
                 "containers": [{
                     "name": "probe", "image": pod_image(install, install["runner_image"]),
                     "command": ["python", "-c", NETWORK_PROBE, json.dumps(targets)],
                     "securityContext": {"allowPrivilegeEscalation": False,
                                         "readOnlyRootFilesystem": True,
                                         "capabilities": {"drop": ["ALL"]}}}]}}
    kubectl("apply", "-f", "-", input=json.dumps(pod))
    deadline = time.monotonic() + 120
    while kubectl("-n", ns, "get", "pod", "network-probe", "-o",
                  "jsonpath={.status.phase}") not in ("Succeeded", "Failed"):
        assert time.monotonic() < deadline, "the network probe did not finish"
        time.sleep(1)
    results = json.loads(kubectl("-n", ns, "logs", "network-probe").strip().splitlines()[-1])
    (install["root"] / "network-probe.json").write_text(json.dumps(results, indent=2))
    assert results["gateway"] == "open" and results["store"] == "open", results
    assert all(results[name] != "open" for name in
               ("control plane", "internet", "kubernetes api")), results
    assert results["seconds until enforced"] < 60
    kubectl("-n", ns, "delete", "pod", "network-probe", "--wait=false")


@pytest.mark.parametrize("overlay", ["eks", "aks", "gke", "onprem"])
def test_every_overlay_renders_and_passes_server_side_validation(kube, overlay):
    """The overlays as shipped, with their REPLACE values filled in, against this cluster's
    API (and Kueue's resource definitions, when installed)."""
    import re

    from kubernetes import client as k8s

    rendered = kubectl("kustomize", str(ROOT / "deploy" / "kustomize" / "overlays" / overlay))
    rendered = (rendered.replace("REPLACE/32", "10.0.0.1/32")
                .replace("sha256:REPLACE", "sha256:" + "a" * 64)
                .replace("REPLACE-registry", "registry.example"))
    assert "REPLACE" in rendered  # the remaining placeholders are values, not structure
    crds = {c.metadata.name for c in
            k8s.ApiextensionsV1Api(kube).list_custom_resource_definition().items}
    if "clusterqueues.kueue.x-k8s.io" not in crds:
        pytest.skip("install Kueue's resource definitions to validate the Kueue component")
    namespace = re.search(r"kind: Namespace\nmetadata:\n(?:  .*\n)*?  name: (\S+)",
                          rendered).group(1)
    # A dry run creates nothing, so the namespaced objects need their namespace to exist.
    kubectl("wait", "--for=delete", f"namespace/{namespace}", "--timeout=120s", check=False)
    created = kubectl("get", "namespace", namespace, check=False) == ""
    if created:
        kubectl("create", "namespace", namespace)
    try:
        out = kubectl("apply", "--dry-run=server", "-f", "-", input=rendered)
    finally:
        if created:
            kubectl("delete", "namespace", namespace, "--wait=true", "--timeout=120s")
    assert out.count("(server dry run)") == rendered.count("\nkind: ")  # every object
