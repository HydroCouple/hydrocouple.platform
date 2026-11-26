#!/usr/bin/env bash
# Run the M4 gate (tests/k8s/test_install_on_kubernetes.py) against the current kubectl
# context, and pack the results to send back (deploy/gate/README.md).
#
#   deploy/gate/run-gate.sh --name eks --overlay deploy/kustomize/overlays/site-eks \
#       --runner REGISTRY/hydrocouple/runner@sha256:... --agent REGISTRY/hydrocouple/agent@sha256:... \
#       --host 10.0.1.23 [--port 18080] [--kubectl kubectl]
#
# --host is this machine's address as the agent pod reaches it; --port is the port the
# cluster may reach it on (default: any free port, which needs no firewall rule only on a
# network that allows everything). Run it from the repository's virtual environment.
set -euo pipefail
repo="$(cd "$(dirname "$0")/../.." && pwd)"
name="" overlay="" runner="" agent="" host="" port="" kubectl_cmd="kubectl"
while [ $# -gt 0 ]; do
  case "$1" in
    --name) name="$2"; shift 2 ;;
    --overlay) overlay="$2"; shift 2 ;;
    --runner) runner="$2"; shift 2 ;;
    --agent) agent="$2"; shift 2 ;;
    --host) host="$2"; shift 2 ;;
    --port) port="$2"; shift 2 ;;
    --kubectl) kubectl_cmd="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
for v in name overlay runner agent host; do
  [ -n "${!v}" ] || { echo "--$v is required" >&2; exit 2; }
done
for ref in "$runner" "$agent"; do
  case "$ref" in *@sha256:*) ;; *) echo "$ref must be an image reference by digest" >&2; exit 2 ;; esac
done
overlay="$(cd "$overlay" && pwd)"
[ -f "$overlay/kustomization.yaml" ] && [ -f "$overlay/agent.json" ] && [ -f "$overlay/release.pub" ] \
  || { echo "$overlay needs kustomization.yaml, agent.json and release.pub" >&2; exit 2; }
# The gate replaces the control plane's address; every other value must be the site's,
# release.pub and the AKS overlay's entra-id.yaml included.
left="$(grep -rn REPLACE "$overlay" | grep -v '"control_plane"' || true)"
[ -z "$left" ] || { echo "fill in these values first:"; echo "$left"; exit 2; } >&2

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
artifacts="${HCP_TEST_ARTIFACTS:-$repo/test_artifacts}"
out="$artifacts/gate-$name-$stamp"
mkdir -p "$out"
kube() { $kubectl_cmd "$@"; }
echo "context: $(kube config current-context)"
kube get nodes -o wide

python - "$out/cluster.json" <<EOF
import json, subprocess, sys
def kube(*args):
    return json.loads(subprocess.run("$kubectl_cmd".split() + list(args), check=True,
                                     capture_output=True, text=True).stdout)
nodes = kube("get", "nodes", "-o", "json")["items"]
crds = [c["metadata"]["name"] for c in kube("get", "crd", "-o", "json")["items"]]
json.dump({
    "name": "$name", "context": subprocess.run("$kubectl_cmd".split() + ["config", "current-context"],
                                               capture_output=True, text=True).stdout.strip(),
    "server_version": kube("version", "-o", "json").get("serverVersion"),
    "nodes": [{"name": n["metadata"]["name"],
               "instance_type": n["metadata"]["labels"].get("node.kubernetes.io/instance-type"),
               "kubelet": n["status"]["nodeInfo"]["kubeletVersion"],
               "kernel": n["status"]["nodeInfo"]["kernelVersion"],
               "cpu": n["status"]["allocatable"]["cpu"],
               "memory": n["status"]["allocatable"]["memory"]} for n in nodes],
    "kueue": "clusterqueues.kueue.x-k8s.io" in crds,
    "runner_image": "$runner", "agent_image": "$agent",
    "commit": subprocess.run(["git", "-C", "$repo", "rev-parse", "HEAD"], capture_output=True,
                             text=True).stdout.strip(),
}, open(sys.argv[1], "w"), indent=2)
EOF

export HCP_TEST_KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
export HCP_TEST_KUBECTL="$kubectl_cmd"
export HCP_TEST_K8S_SITE_OVERLAY="$overlay"
export HCP_TEST_K8S_RUNNER_IMAGE="$runner" HCP_TEST_K8S_AGENT_IMAGE="$agent"
export HCP_TEST_CONTROL_PLANE_HOST="$host"
[ -z "$port" ] || export HCP_TEST_CONTROL_PLANE_PORT="$port"
unset HCP_TEST_K8S_POD_REGISTRY HCP_TEST_K8S_IMAGE_KEY

status=0
(cd "$repo" && python -m pytest tests/k8s/test_install_on_kubernetes.py -p no:cacheprovider -rA \
    --junitxml "$out/junit.xml") 2>&1 | tee "$out/pytest.log" || status=$?
for f in gate.json verify.json network-probe.json agent.log events.txt; do
  cp "$artifacts/test_install_on_kubernetes/$f" "$out/" 2>/dev/null || true
done
tar -C "$(dirname "$out")" -czf "$out.tar.gz" "$(basename "$out")"
echo "results: $out.tar.gz"
exit "$status"
