"""The M4 gate: the agent installed in a cluster from ``deploy/kustomize``, as a site would.

One installation serves the module:
- **Control plane:** runs in this process, over HTTP on an address pods can reach.
- **Store:** S3 (moto, on the node), or the site's store.
- **Agent:** runs in the cluster from its signed image. By default it is installed by an
  overlay this module writes over ``deploy/kustomize/base``, with the store's credentials
  in a Secret. With ``HCP_TEST_K8S_SITE_OVERLAY`` it is installed by a site's filled-in
  copy of a cloud overlay instead (``deploy/gate/README.md``), with that overlay's
  namespace, store and workload identities.

The tests check the installation the way onboarding does:
- ``hcp agent --verify`` from inside the agent pod passes, including a runner pod that
  writes to the store;
- the runner network policy lets runner pods reach DNS, the agent's gateway and the
  store, and nothing else.

Then the plan's M4 gate, through the HTTP API as a client would drive it:
- upload, run, disconnect (with the agent pod restarted mid-run), reconnect, inspect,
  approve and delete;
- a study on an evaluator pool, through the gateway;
- numerical parity with a local pool;
- revocation, last, because it stops the agent.

The gate's report is written to ``test_artifacts/test_install_on_kubernetes/gate.json``.

Besides the cluster (``HCP_TEST_KUBECONFIG``) this needs:
- ``HCP_TEST_K8S_AGENT_IMAGE``, ``HCP_TEST_K8S_RUNNER_IMAGE`` and ``HCP_TEST_K8S_IMAGE_KEY``:
  both images by digest, and the cosign public key that signed them (a site overlay
  carries its own key);
- ``HCP_TEST_K8S_POD_REGISTRY``: the registry's address as pods reach it (the agent pulls
  through it as its mirror), when the image references name an address only the node
  can reach, such as ``127.0.0.1:5000``;
- ``HCP_TEST_KUBECTL``: the kubectl command (default ``kubectl``; ``k3s kubectl`` on k3s);
- on a cloud: ``HCP_TEST_K8S_SITE_OVERLAY``, ``HCP_TEST_CONTROL_PLANE_HOST`` (this
  machine's address as the agent pod reaches it; on a one-node cluster it defaults to the
  node's) and optionally ``HCP_TEST_CONTROL_PLANE_PORT`` (default a free port).

When the module ends it deletes everything the overlay installed, the namespace included.
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


def namespace_phase(namespace: str) -> str:
    """``Active``, or ``""`` when the namespace does not exist, after waiting for any deletion
    in progress."""
    get = ("get", "namespace", namespace, "-o", "jsonpath={.status.phase}")
    if kubectl(*get, check=False) == "Terminating":
        kubectl("wait", "--for=delete", f"namespace/{namespace}", "--timeout=280s", check=False)
    return kubectl(*get, check=False)


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


def write_site_overlay(path: Path, site: Path, agent_image: str, config: dict) -> None:
    """The site's overlay, with the image under test and this control plane's address.
    The generator names the namespace, because a replacement matches on it."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "agent.json").write_text(json.dumps(config, indent=2))
    (path / "kustomization.yaml").write_text(f"""\
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - {os.path.relpath(site, path)}
configMapGenerator:
  - name: hcp-agent-config
    namespace: {config["kubernetes"]["namespace"]}
    behavior: replace
    files: [agent.json]
patches:
  - target: {{kind: Deployment, name: hcp-agent}}
    patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: {agent_image}
""")


def store_address(uri: str) -> tuple[str, int]:
    """The store's host and port as runner pods reach it."""
    from urllib.parse import urlparse

    from hydrocouple_platform.storage import split_uri

    scheme = urlparse(uri).scheme
    (first, *_), _, query = split_uri(uri, 1)
    endpoint = query.get("endpoint_url") or query.get("endpoint")
    if endpoint:
        parsed = urlparse(endpoint)
        return parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)
    return {"s3": (f"s3.{query.get('region', 'us-east-1')}.amazonaws.com", 443),
            "azure": (f"{first}.blob.core.windows.net", 443),
            "gs": ("storage.googleapis.com", 443)}[scheme]


class ControlPlane:
    """The control plane for the module: services, the HTTP API on all addresses, and the
    dispatcher and reconciler in a thread."""

    def __init__(self, database_url: str, jwks_file: Path, workflow_images: dict,
                 port: int | None = None):
        import uvicorn

        from hydrocouple_platform.api import create_app
        from hydrocouple_platform.config import ControlPlaneSettings
        from hydrocouple_platform.db import make_engine, make_sessionmaker
        from hydrocouple_platform.services import PlatformServices

        self.settings = ControlPlaneSettings(
            database_url=database_url, jwt_issuer=ISSUER, jwt_audience=AUDIENCE,
            jwks_file=str(jwks_file), attempt_token_secret="s" * 48, lease_seconds=30,
            abandon_after_seconds=120, heartbeat_interval_seconds=1.0,
            deletion_writer_wait_seconds=120, deletion_recheck_seconds=5,
            workflow_images=workflow_images)
        self.engine = make_engine(database_url)
        self.services = PlatformServices(make_sessionmaker(self.engine), self.settings)
        self.port = port or free_port("0.0.0.0")
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
    mirror = os.environ.get("HCP_TEST_K8S_POD_REGISTRY")
    site = os.environ.get("HCP_TEST_K8S_SITE_OVERLAY")
    root = ARTIFACTS / "test_install_on_kubernetes"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)

    host = os.environ.get("HCP_TEST_CONTROL_PLANE_HOST")
    if not site:
        node = node_address(kube)
        host = host or node
    elif not host:
        pytest.skip("set HCP_TEST_CONTROL_PLANE_HOST to this machine's address in the "
                    "cluster's network")
    cluster, admin = pg_template
    database = "t_" + uuid.uuid4().hex[:12]
    with admin.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {database} TEMPLATE hcp_template"))
    plane = ControlPlane(cluster.url.replace("/postgres?", f"/{database}?"), jwks_file,
                         {"swmm.simulation@1": runner_image, "gym.evaluate@1": runner_image},
                         port=int(os.environ.get("HCP_TEST_CONTROL_PLANE_PORT", "0")) or None)
    control_plane = f"http://{host}:{plane.port}"
    overlay, store, environment = root / "overlay", None, pytest.MonkeyPatch()
    namespace = None
    try:
        world = make_world(plane.services, root / "world")
        secrets = {"hcp-agent-credential": {"credential": world.credential}}
        if site:
            config = json.loads((Path(site) / "agent.json").read_text())
            config["control_plane"] = control_plane
            store_uri = config["store"]
            if namespace_phase(config["kubernetes"]["namespace"]) == "Active":
                pytest.fail(f"namespace {config['kubernetes']['namespace']} exists: the gate "
                            "installs its own agent there and deletes it when it ends, so it "
                            "needs a cluster without one")
            write_site_overlay(overlay, Path(site).resolve(), agent_image, config)
        else:
            store, store_port = moto(host="0.0.0.0")
            for k, v in CREDENTIALS.items():
                environment.setenv(k, v)
            boto3.client("s3", endpoint_url=f"http://{node}:{store_port}").create_bucket(
                Bucket="hcp-site")
            store_uri = (f"s3://hcp-site/platform?endpoint_url=http://{node}:{store_port}"
                         "&region=us-east-1")
            config = _onprem_config(control_plane, store_uri, "hci-" + uuid.uuid4().hex[:6],
                                    mirror)
            write_overlay(overlay, config["kubernetes"]["namespace"], agent_image, config,
                          Path(_env("HCP_TEST_K8S_IMAGE_KEY")), node, store_port)
            secrets["hcp-store-credentials"] = CREDENTIALS
        namespace = config["kubernetes"]["namespace"]
        kubectl("apply", "-k", str(overlay))
        for name, data in secrets.items():
            kubectl("apply", "-f", "-", input=json.dumps({
                "apiVersion": "v1", "kind": "Secret",
                "metadata": {"name": name, "namespace": namespace}, "stringData": data}))
        kubectl("-n", namespace, "rollout", "status", "deploy/hcp-agent", "--timeout=280s")
        yield {"plane": plane, "world": world, "namespace": namespace,
               "store": store_address(store_uri), "store_uri": store_uri,
               "runner_image": runner_image, "mirror": mirror, "root": root, "config": config}
    finally:
        if namespace is not None:
            (root / "agent.log").write_text(kubectl(
                "-n", namespace, "logs", "deploy/hcp-agent", "--tail=-1", check=False))
            (root / "events.txt").write_text(kubectl(
                "-n", namespace, "get", "events", "--sort-by=.lastTimestamp", check=False))
            # Everything the overlay installed, its cluster-wide Kueue objects included.
            kubectl("delete", "-k", str(overlay), "--wait=false", "--ignore-not-found",
                    check=False)
        if store is not None:
            store.kill()
        plane.close()
        environment.undo()


def _onprem_config(control_plane: str, store_uri: str, namespace: str, mirror) -> dict:
    return {
        "control_plane": control_plane, "store": store_uri,
        "egress": "summaries", "adapter": "kubernetes",
        "kubernetes": {"namespace": namespace, "max_cpu": 2, "max_memory_gib": 4,
                       "scratch_gib": 8, "slots": 4, "poll_seconds": 1, "grace_seconds": 10,
                       "runner_env_secret": "hcp-store-credentials"},
        "image_trust": {"keys": ["/etc/hydrocouple/keys/release.pub"], "mirror": mirror,
                        "allow_http_registry": True},
        "gateway": {"host": "0.0.0.0", "port": 8081,
                    "url": f"http://hcp-agent-gateway.{namespace}.svc:8081"},
    }


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
    _record("capabilities", **caps)


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
    from urllib.parse import urlparse

    ns = install["namespace"]
    plane = urlparse(install["config"]["control_plane"])
    targets = [["gateway", f"hcp-agent-gateway.{ns}.svc", 8081],
               ["store", *install["store"]],
               ["control plane", plane.hostname, plane.port],
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

    import yaml
    from kubernetes import client as k8s

    rendered = kubectl("kustomize", str(ROOT / "deploy" / "kustomize" / "overlays" / overlay))
    # Every namespaced object, the overlay's generated ConfigMaps included, lands in the
    # agent's namespace (found by the site-mode gate: they had none, so went to default).
    agent = json.loads((ROOT / "deploy" / "kustomize" / "overlays" / overlay / "agent.json")
                       .read_text())
    cluster_scoped = {"Namespace", "ClusterQueue", "ResourceFlavor"}
    placed = {(d["kind"], d["metadata"]["name"]): d["metadata"].get("namespace")
              for d in yaml.safe_load_all(rendered) if d and d["kind"] not in cluster_scoped}
    assert set(placed.values()) == {agent["kubernetes"]["namespace"]}, placed
    # The Deployment mounts the generated ConfigMaps under their generated names.
    (deployment,) = [n for (kind, n) in placed if kind == "Deployment"]
    docs = {(d["kind"], d["metadata"]["name"]): d for d in yaml.safe_load_all(rendered) if d}
    mounted = {v["configMap"]["name"] for v in
               docs[("Deployment", deployment)]["spec"]["template"]["spec"]["volumes"]
               if "configMap" in v}
    assert mounted <= {n for (kind, n) in placed if kind == "ConfigMap"}, mounted
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
    created = namespace_phase(namespace) == ""
    if created:
        kubectl("create", "namespace", namespace)
    try:
        out = kubectl("apply", "--dry-run=server", "-f", "-", input=rendered)
    finally:
        if created:
            kubectl("delete", "namespace", namespace, "--wait=true", "--timeout=120s")
    assert out.count("(server dry run)") == rendered.count("\nkind: ")  # every object


# -- the M4 gate ------------------------------------------------------------------------

GATE: dict = {}
MODEL = ROOT / "tests" / "data" / "site_drainage_example.inp"


def _record(step: str, **facts) -> None:
    GATE[step] = facts
    path = ARTIFACTS / "test_install_on_kubernetes" / "gate.json"
    path.write_text(json.dumps(GATE, indent=2, default=str))


def _client(install, token: str):
    from hydrocouple_platform.client import PlatformClient

    return PlatformClient(f"http://127.0.0.1:{install['plane'].port}", token)


HELD = ("LEASED", "QUEUED", "STARTING", "RUNNING", "FINALIZING")
TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED")


def _wait_for(client, job_id: str, states, timeout: float = 300) -> dict:
    """The job once it is in one of ``states`` or has ended."""
    deadline = time.monotonic() + timeout
    while True:
        job = client.job(job_id)
        if job["state"] in states or job["state"] in TERMINAL or time.monotonic() > deadline:
            return job
        time.sleep(0.25)


# Every object, version and delete marker under a prefix, listed with the agent's identity.
STORE_LISTING = """
import json, sys
from hydrocouple_platform.storage import open_store
store = open_store(sys.argv[1])
name = store.prefix + sys.argv[2]
kind = type(store).__name__
if kind == "S3Store":
    pages = store.client.get_paginator("list_object_versions").paginate(
        Bucket=store.bucket, Prefix=name)
    keys = [v["Key"] for p in pages for v in p.get("Versions", []) + p.get("DeleteMarkers", [])]
elif kind == "AzureBlobStore":
    try:
        blobs = list(store._container.list_blobs(name_starts_with=name, include=["versions"]))
    except Exception:
        blobs = list(store._container.list_blobs(name_starts_with=name))
    keys = [b.name for b in blobs]
else:
    keys = [b.name for b in store.client.list_blobs(store.bucket, prefix=name, versions=True)]
print(json.dumps(keys))
"""


def _store_keys(install, prefix: str) -> list[str]:
    out = kubectl("-n", install["namespace"], "exec", "deploy/hcp-agent", "--", "python", "-c",
                  STORE_LISTING, install["store_uri"], prefix)
    return json.loads(out.strip().splitlines()[-1])


def test_gate_upload_run_disconnect_reconnect_inspect_approve_delete(install, make_token):
    from hydrocouple_platform.contracts.requests import Budget, RunRequest
    from hydrocouple_platform.packaging import package_swmm_model

    world, ns = install["world"], install["namespace"]
    token = make_token("alice")

    # Upload: the client packages the model and uploads through the agent's S3 grants.
    with _client(install, token) as client:
        revision = client.upload_revision(
            package_swmm_model(MODEL, install["root"] / "package"),
            project_id=world.project_id, target_id=world.target_id)
        assert revision["state"] == "READY"
        _record("upload", revision_id=revision["revision_id"],
                manifest_sha256=revision["manifest_sha256"])

        # Run, then disconnect: the client goes away, and so does the agent pod.
        accepted = client.submit_run(RunRequest(
            project_id=world.project_id, revision_id=revision["revision_id"],
            workflow="swmm.simulation", workflow_version="1", target_id=world.target_id),
            "gate-run-0001")
        # Submitted to the cluster and not yet published, so the new agent must adopt it.
        held = _wait_for(client, accepted["job_id"], ("STARTING", "RUNNING", "FINALIZING"))
        assert held["state"] in HELD, held
    kubectl("-n", ns, "delete", "pod", "-l", "app.kubernetes.io/name=hcp-agent", "--wait=true")
    kubectl("-n", ns, "rollout", "status", "deploy/hcp-agent", "--timeout=180s")
    _record("disconnect", state_when_disconnected=held["state"],
            agent_pod="deleted and recreated by its Deployment")

    # Reconnect with a new client and inspect: the new agent adopted the running Job.
    with _client(install, make_token("alice")) as client:
        job = client.wait_for_job(accepted["job_id"], timeout=600)
        assert job["state"] == "SUCCEEDED", job
        assert len(job["attempts"]) == 1, job["attempts"]  # adopted, not retried
        attempt_id = job["attempts"][0]["attempt_id"]
        assert f"adopted attempt {attempt_id}" in kubectl(
            "-n", ns, "logs", "deploy/hcp-agent", "--tail=-1")
        results = client.results(accepted["run_id"])
        assert results["provenance"]["runtime"]["image"].endswith(
            install["runner_image"].split("@")[1])
        names = {a["path"] for a in results["artifacts"]}
        assert {"run.json", "model.out", "model.rpt"} <= names
        _record("reconnect and inspect", job_state=job["state"], adopted=attempt_id,
                attempts=len(job["attempts"]), artifacts=sorted(names),
                image=results["provenance"]["runtime"]["image"])

        # Approve: an AI agent session beyond its budget waits for a person.
        session = client.create_agent_session(
            project_id=world.project_id, client_name="claude-code",
            budget=Budget(max_runs=0).model_dump())
    with _client(install, session["agent_session_token"]) as agent_client:
        pending = agent_client.submit_run(RunRequest(
            project_id=world.project_id, revision_id=revision["revision_id"],
            workflow="swmm.simulation", workflow_version="1", target_id=world.target_id),
            "gate-run-0002")
        assert pending["status"] == "PENDING_APPROVAL"
    with _client(install, make_token("alice")) as client:
        client.resolve_approval(pending["approval_id"], True, "gate: approved by a person")
        approved = client.wait_for_job(pending["job_id"], timeout=600)
        assert approved["state"] == "SUCCEEDED", approved
        _record("approve", status_before=pending["status"], job_state=approved["state"])

        # Delete: the run's objects are erased from S3; the revision stays.
        prefix = f"runs/{world.org_id}/{world.project_id}/{accepted['run_id']}/"
        assert _store_keys(install, prefix)
        deletion = client.delete_run(accepted["run_id"])
        done = client.wait_for_deletion(deletion["deletion_id"], timeout=300)
        assert done["state"] == "ERASED", done
        assert _store_keys(install, prefix) == []
        assert _store_keys(install, f"revisions/{world.org_id}/{world.project_id}/"
                                    f"{revision['revision_id']}/")
        _record("delete", deletion=done["state"], passes=done["passes"],
                objects_deleted=done["objects_deleted"])
    GATE["revision_id"] = revision["revision_id"]


def test_gate_a_study_runs_on_a_pool_through_the_installed_agents_gateway(install, make_token):
    pytest.importorskip("openswmm_gymnasium.optimize")
    from openswmm_gymnasium.optimize import OptimizationConfig
    from openswmm_gymnasium.spec.config import EnvConfig
    from test_platform_on_kubernetes import env_config

    from hydrocouple_platform.gym import run_platform_study
    from hydrocouple_platform.packaging import package_swmm_model

    world = install["world"]
    model = ROOT / "tests" / "data" / "twin_tank_heavy" / "model.inp"
    opt = OptimizationConfig(algorithm="random_search", budget=12, seed=5, workers=0,
                             on_failure="penalty")
    with _client(install, make_token("alice")) as client:
        revision = client.upload_revision(
            package_swmm_model(model, install["root"] / "twin-tank"),
            project_id=world.project_id, target_id=world.target_id)
        result = run_platform_study(
            EnvConfig.model_validate(env_config("model/model.inp")), opt,
            install["root"] / "study", client=client, project_id=world.project_id,
            revision_id=revision["revision_id"], target_id=world.target_id,
            inp_path="model/model.inp", pool_size=1, batch_size=6)
    assert result["evaluations_count"] == 12 and result["failed_count"] == 0
    _record("study", evaluations=result["evaluations_count"], pareto=len(result["pareto"]))


def test_gate_results_match_a_local_pool_run_of_the_same_revision(install, make_token):
    """Numerical parity: the same model, workflow and thread count, on a local pool."""
    import hashlib

    from hydrocouple_platform.agent import Agent
    from hydrocouple_platform.agent.adapters.local import LocalPoolAdapter
    from hydrocouple_platform.contracts.jobs import EgressPolicy
    from hydrocouple_platform.contracts.requests import RunRequest, TargetCreate
    from hydrocouple_platform.packaging import package_swmm_model
    from hydrocouple_platform.storage import LocalStore
    from world import DirectAgentClient

    world, svc = install["world"], install["plane"].services
    local_root = install["root"] / "local-pool"
    store_uri = "file://" + str((local_root / "store").resolve())
    local = svc.register_target(world.admin, world.org_id, TargetCreate(
        name="workstation", ownership_mode="organization-owned", adapter="local-pool",
        storage_uri=store_uri))
    local_agent = Agent(DirectAgentClient(svc, local["target_id"]),
                        LocalPoolAdapter(local_root / "work"), LocalStore(local_root / "store"),
                        store_uri, EgressPolicy.SUMMARIES)
    stop = threading.Event()
    thread = threading.Thread(target=local_agent.run_forever, args=(stop, 0.5), daemon=True)
    thread.start()
    outputs, timings = {}, {}
    try:
        with _client(install, make_token("alice")) as client:
            for name, target in (("kubernetes", world.target_id),
                                 ("local pool", local["target_id"])):
                revision = client.upload_revision(
                    package_swmm_model(MODEL, install["root"] / f"parity-{name}"),
                    project_id=world.project_id, target_id=target)
                submitted = time.monotonic()
                accepted = client.submit_run(RunRequest(
                    project_id=world.project_id, revision_id=revision["revision_id"],
                    workflow="swmm.simulation", workflow_version="1", target_id=target,
                    parameters={"threads": 1}), f"gate-parity-{name}")
                _wait_for(client, accepted["job_id"], ("RUNNING", "FINALIZING"))
                running = time.monotonic()
                job = client.wait_for_job(accepted["job_id"], timeout=600)
                assert job["state"] == "SUCCEEDED", (name, job)
                timings[name] = {"seconds to running": round(running - submitted, 1),
                                 "seconds to published": round(time.monotonic() - submitted, 1)}
                files = client.download(accepted["run_id"], ["model.out"],
                                        install["root"] / f"parity-{name}-out")
                outputs[name] = {
                    "model.out sha256": hashlib.sha256(
                        files["model.out"].read_bytes()).hexdigest(),
                    "quality": client.results(accepted["run_id"])["quality"]}
    finally:
        stop.set()
        thread.join(timeout=30)
        local_agent.close()
    k8s_run, local_run = outputs["kubernetes"], outputs["local pool"]
    identical = k8s_run["model.out sha256"] == local_run["model.out sha256"]
    _record("numerical parity", bitwise_identical=identical, **outputs)
    _record("timings", **timings)  # client-observed, at 0.25 s resolution
    # Parity at pinned threads: the same solver path and continuity. Different CPUs or C
    # libraries may differ in the last bits, so model.out's hash is recorded, not required.
    a, b = k8s_run["quality"], local_run["quality"]
    assert a["threads"] == b["threads"] and a["routing_steps"] == b["routing_steps"], (a, b)
    for kind, error in a["continuity_errors"].items():
        assert error == pytest.approx(b["continuity_errors"][kind], rel=1e-6, abs=1e-12), kind


def test_gate_revocation_drains_in_flight_work_and_claims_nothing_new(install, make_token):
    """Last, because it stops the agent: a revoked target's agent finishes the attempt it
    holds, claims nothing new, and stops."""
    from hydrocouple_platform.contracts.errors import PlatformError
    from hydrocouple_platform.contracts.requests import Budget, RunRequest
    from hydrocouple_platform.packaging import package_swmm_model

    world, ns = install["world"], install["namespace"]
    with _client(install, make_token("alice")) as client:
        revision_id = GATE.get("revision_id") or client.upload_revision(
            package_swmm_model(MODEL, install["root"] / "revoke"),
            project_id=world.project_id, target_id=world.target_id)["revision_id"]
        request = RunRequest(project_id=world.project_id, revision_id=revision_id,
                             workflow="swmm.simulation", workflow_version="1",
                             target_id=world.target_id)
        session = client.create_agent_session(
            project_id=world.project_id, client_name="claude-code",
            budget=Budget(max_runs=0).model_dump())
        with _client(install, session["agent_session_token"]) as agent_client:
            # Waiting for a person, so it can only become leasable after revocation.
            later = agent_client.submit_run(
                request.model_copy(update={"parameters": {"threads": 2}}), "gate-revoke-0002")
        assert later["status"] == "PENDING_APPROVAL"
        in_flight = client.submit_run(request, "gate-revoke-0001")
        held = _wait_for(client, in_flight["job_id"], HELD)
        assert held["state"] in HELD, held

        client.revoke_target(world.target_id)
        client.resolve_approval(later["approval_id"], True, "gate: approved after revocation")
        with pytest.raises(PlatformError) as refused:
            client.submit_run(request.model_copy(update={"parameters": {"threads": 3}}),
                              "gate-revoke-0003")
        finished = client.wait_for_job(in_flight["job_id"], timeout=600)
        assert finished["state"] == "SUCCEEDED", finished  # in-flight work still publishes
        assert len(finished["attempts"]) == 1
        time.sleep(10)  # several of the agent's poll intervals
        unclaimed = client.job(later["job_id"])
    assert unclaimed["state"] == "LEASABLE" and unclaimed["attempts"] == [], unclaimed
    log = kubectl("-n", ns, "logs", "deploy/hcp-agent", "--tail=-1", check=False)
    previous = kubectl("-n", ns, "logs", "deploy/hcp-agent", "--previous", "--tail=-1",
                       check=False)
    assert "its work has drained; stopping" in log + previous
    _record("revocation", state_when_revoked=held["state"], in_flight=finished["state"],
            approved_after=unclaimed["state"],
            submitted_after=str(refused.value))
