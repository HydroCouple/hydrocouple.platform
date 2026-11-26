# The M4 gate on EKS, AKS and GKE

The gate is `tests/k8s/test_install_on_kubernetes.py`. It installs the agent from an overlay and checks it as onboarding does (`hcp agent --verify`, the reported capabilities, the runner network policy). Then it drives the platform through the HTTP API as a client would:
- upload, run, disconnect (the agent pod is deleted mid-run), reconnect, inspect, approve and delete;
- a study on an evaluator pool, through the agent's gateway;
- numerical parity with a local pool;
- revocation, last.

It passes on k3s in the development workspace. These runbooks run it on each cloud: [EKS](eks.md), [AKS](aks.md), [GKE](gke.md).

## How it runs

The control plane runs inside the test process, on a **gate machine**, with its own PostgreSQL. The agent pod connects to it over plain HTTP, and the test's client uploads and downloads straight to the store through the agent's grants. So the gate machine must be in the cluster's network, and reach the store the way the cluster does.

```
gate machine (a VM in the cluster's VPC or VNet)        cluster
  pytest: control plane + PostgreSQL  <---- HTTP ----   agent pod  ---->  runner pods
  test client  -------- grants -------->  store  <-------------------------'
```

The gate installs its agent into the overlay's namespace (`hydrocouple`), and when it ends deletes everything the overlay installed, that namespace and the Kueue queues included. Use a cluster with no other HydroCouple installation; the gate stops if the namespace already exists.

## What you need

**A cluster per cloud.** Kubernetes 1.31 or later (the Job pod failure policy is GA there), NetworkPolicy enforced, and workload identity. Each cloud runbook says how.

**A gate machine.** A Linux x86_64 VM in the cluster's network, which you reach over SSH (through a bastion, Session Manager or IAP, as you prefer), with:
- Docker, git, kubectl, the cloud's CLI and OpenTofu (or Terraform 1.6+);
- Python 3.10, because the engine wheel is built for CPython 3.10 (Ubuntu 22.04 ships it; elsewhere `uv python install 3.10`);
- [cosign](https://github.com/sigstore/cosign/releases) and [syft](https://github.com/anchore/syft/releases) on `PATH`;
- checkouts of `hydrocouple.platform` (the commit under test) and `openswmm.gymnasium`;
- the OpenSWMM engine wheel for CPython 3.10 on Linux x86_64 (`openswmm-6.0.0a4…-cp310-cp310-linux_x86_64.whl`) in a directory, here `~/wheels`. The runner image and the local pool both install it, so the parity check compares the same engine build.

PostgreSQL comes with the test dependencies (`pgserver`).

## Steps

The cloud runbook and this page take turns:
1. **Cloud runbook §1 and §2:** the cluster, and the gate machine with access to it.
2. **Here, on the gate machine:** A and B.
3. **Cloud runbook §3 and §4:** the store and the registry. Sign in to the registry on the gate machine.
4. **Here:** C and D, the images.
5. **Cloud runbook §5:** the overlay, in the gate machine's checkout.
6. **Here:** E to H.

### A. Set up the gate machine

```bash
cd hydrocouple.platform
python3.10 -m venv .venv && . .venv/bin/activate
pip install --find-links ~/wheels "$HOME/src/openswmm.gymnasium[spec,platypus]" -e '.[dev,gym]'
```

### B. Make a signing key

```bash
cosign generate-key-pair      # cosign.key (keep it private) and cosign.pub
export COSIGN_PASSWORD=...    # the password you chose
```

### C. Build the base image

The engine links `libgomp` and `libsqlite3`, so the base is the official Python image with both added. `REGISTRY` comes from the cloud runbook.

```bash
docker pull python:3.10-slim-bookworm
python_ref=$(docker inspect --format '{{index .RepoDigests 0}}' python:3.10-slim-bookworm)
mkdir -p ~/base && cat > ~/base/Dockerfile <<EOF
FROM $python_ref
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 libsqlite3-0 \
 && rm -rf /var/lib/apt/lists/*
EOF
docker build -t "$REGISTRY/hydrocouple/base:3.10" ~/base
docker push "$REGISTRY/hydrocouple/base:3.10"
BASE=$(docker inspect --format '{{index .RepoDigests 0}}' "$REGISTRY/hydrocouple/base:3.10")
```

### D. Build, sign and push the runner and agent images

```bash
images/build.sh runner --base "$BASE" --image "$REGISTRY/hydrocouple/runner" --tag gate \
    --python python3.10 --find-links ~/wheels --local ~/src/openswmm.gymnasium \
    --key cosign.key --no-tlog
images/build.sh agent --base "$BASE" --image "$REGISTRY/hydrocouple/agent" --tag gate \
    --python python3.10 --key cosign.key --no-tlog
RUNNER=$(cat test_artifacts/images/runner-gate/image.ref)
AGENT=$(cat test_artifacts/images/agent-gate/image.ref)
```

`--no-tlog` keeps the signatures out of the public transparency log. The agent checks signatures against its pinned keys only, unless `require_transparency_log` is set.

### E. Install Kueue

```bash
kubectl apply --server-side -f \
    https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.1/manifests.yaml
kubectl -n kueue-system rollout status deploy/kueue-controller-manager
```

The overlays' Kueue component admits up to 64 CPUs and 256 GiB. That is more than the gate uses.

### F. Run the gate

`--host` is the gate machine's private address. `--port` is the port the cloud runbook opened to the cluster.

```bash
deploy/gate/run-gate.sh --name eks --overlay deploy/kustomize/overlays/site-eks \
    --runner "$RUNNER" --agent "$AGENT" --host "$(hostname -I | awk '{print $1}')" --port 18080
```

It takes about ten minutes, most of it the image pulls of the first runs.

### G. Send back the results

The script prints the path of `test_artifacts/gate-<name>-<time>.tar.gz`. Send that file. It contains:
- `gate.json`: each step's result, the capabilities the agent reported, timings, and the parity hashes;
- `pytest.log` and `junit.xml`;
- `verify.json`, `network-probe.json`, the agent's log and the namespace's events;
- `cluster.json`: the Kubernetes version, node types, and whether Kueue is installed.

It contains no credentials. The agent credential exists only in the test process and the namespace's Secret, which the gate deletes.

### H. Clean up

The gate deletes what it installed in the cluster. After all three clouds have run, remove the rest: `tofu destroy` in each module, the images, the private endpoints, and the clusters if you made them for the gate.

## Reading the results

**Every test passes.** `pytest.log` ends with `passed` and no failures or errors.

**Parity.** The test requires the same thread counts and routing steps for `kubernetes` and `local pool`, and continuity errors that agree to a relative 1e-6. The local pool runs on the gate machine and the runner on a node, both with one solver thread. `gate.json` also records whether `model.out` is bitwise identical (`bitwise_identical`). It was on k3s, where both ran on one machine. On a cloud, different CPUs or C libraries may change the last bits; that is worth knowing but doesn't fail the gate.

**Timings.** `seconds to running` is measured by the client from submission, so it includes the image pull on a node that has not pulled the runner yet.

**Common failures:**

| Symptom | Cause |
| --- | --- |
| `verify.json`: `image signature` fails with an authorization error | The agent's identity can't read the registry. See the cloud runbook's registry step. |
| `verify.json`: `storage` fails | The agent's identity, or the store's endpoint as the agent pod resolves it |
| `network-probe.json`: `store` is not `open` | `hcp-runner-storage` doesn't name the store endpoint's CIDRs, or the store's name doesn't resolve to them |
| The capabilities test times out: the agent never polled | The gate machine's port isn't open to the cluster, or `--host` is not the address pods reach |
| A run never reaches `RUNNING` | Kueue isn't admitting the Job (`kubectl -n hydrocouple get workloads`), or the nodes can't pull the runner image (`kubectl -n hydrocouple get events`) |
