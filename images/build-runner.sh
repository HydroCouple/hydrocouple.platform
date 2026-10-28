#!/usr/bin/env bash
# Build, push, describe and sign the runner image.
#
#   images/build-runner.sh --base REGISTRY/python@sha256:... --image REGISTRY/hydrocouple/runner \
#       --tag 0.1.0 --python python3.10 [--find-links DIR]... [--local PATH]... \
#       [--key cosign.key] [--no-tlog] [--http-registry] [--out DIR]
#
# The wheelhouse is built with --python, which must match the base image's Python.
# --find-links adds directories of local wheels (an unpublished engine build); --local
# adds source checkouts to build (openswmm.gymnasium). With --key the image is signed with
# cosign and its SPDX SBOM (made by syft) attached as an attestation. The image digest is
# printed last, for the control plane's workflow registry (HCP_WORKFLOW_IMAGES).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(dirname "$here")"
base="" image="" tag="dev" python="python3" key="" out="" tlog=1 http=0
links=() locals=()
while [ $# -gt 0 ]; do
  case "$1" in
    --base) base="$2"; shift 2 ;;
    --image) image="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --python) python="$2"; shift 2 ;;
    --find-links) links+=("$2"); shift 2 ;;
    --local) locals+=("$2"); shift 2 ;;
    --key) key="$2"; shift 2 ;;
    --no-tlog) tlog=0; shift ;;
    --http-registry) http=1; shift ;;
    --out) out="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$base" ] && [ -n "$image" ] || { echo "--base and --image are required" >&2; exit 2; }
case "$base" in *@sha256:*) ;; *) echo "--base must be pinned by digest" >&2; exit 2 ;; esac
out="${out:-$repo/test_artifacts/images/runner-$tag}"
ctx="$out/context"
rm -rf "$ctx" && mkdir -p "$ctx/wheelhouse"
cp "$here/runner/Dockerfile" "$ctx/"

# The wheelhouse: this package with the runner and gym extras, and everything they need.
"$python" -m venv "$out/build-venv"
"$out/build-venv/bin/pip" install -q --upgrade pip wheel
args=()
for d in "${links[@]}"; do args+=(--find-links "$d"); done
for p in "${locals[@]}"; do
  "$out/build-venv/bin/pip" wheel -q --no-deps --wheel-dir "$ctx/wheelhouse" "$p"
done
"$out/build-venv/bin/pip" wheel -q --wheel-dir "$ctx/wheelhouse" --find-links "$ctx/wheelhouse" \
  "${args[@]}" "$repo[runner,gym,s3,azure,gcs]"

docker build --build-arg BASE="$base" -t "$image:$tag" "$ctx"
docker push "$image:$tag" >/dev/null
ref="$(docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$image:$tag" \
       | grep "^$image@" | head -1)"
[ -n "$ref" ] || { echo "could not find the pushed digest of $image:$tag" >&2; exit 1; }

if [ -n "$key" ]; then
  flags=()
  [ "$tlog" = 1 ] || flags+=(--tlog-upload=false)
  [ "$http" = 1 ] && flags+=(--allow-http-registry --allow-insecure-registry)
  sbom="$out/sbom.spdx.json"
  SYFT_REGISTRY_INSECURE_USE_HTTP=$([ "$http" = 1 ] && echo true || echo false) \
    syft -q "registry:$ref" -o "spdx-json=$sbom"
  COSIGN_PASSWORD="${COSIGN_PASSWORD:-}" cosign sign -y --key "$key" "${flags[@]}" "$ref"
  COSIGN_PASSWORD="${COSIGN_PASSWORD:-}" cosign attest -y --key "$key" "${flags[@]}" \
    --type spdxjson --predicate "$sbom" "$ref"
fi
echo "$ref" | tee "$out/image.ref"
