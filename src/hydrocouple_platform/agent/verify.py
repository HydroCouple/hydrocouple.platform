"""``hcp agent --verify`` and ``hcp agent --dry-run``: check a site, and say what the agent does.

Verification checks, in order:
1. the control plane is reachable and accepts the target credential;
2. the scheduler permissions are in place: the namespaced Role on Kubernetes, a writable
   work root on the local pool;
3. the store accepts a write, reports its size and SHA-256, and deletes it;
4. with ``--image``: the image is signed by a pinned key, and a runner pod built
   exactly as attempts' pods are starts under the site's pod rules and network
   policy, and writes to the store with the runner's own identity.

The dry run prints every call the agent makes, without making any.
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from .. import __version__
from .config import AgentConfig, build_adapter

VERIFY_PREFIX = "runs/agent-verify/"


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


def verify(cfg: AgentConfig, credential: str, image: str | None = None,
           kubeconfig: str | None = None) -> list[Check]:
    import httpx

    from ..contracts.agent import PollRequest
    from ..contracts.errors import PlatformError
    from ..storage import open_store, sha256_file
    from .agent import AgentClient
    from .imagetrust import ImageVerifier, UntrustedImage

    checks: list[Check] = []
    adapter = build_adapter(cfg, kubeconfig)
    try:
        AgentClient(cfg.control_plane, credential).poll(PollRequest(
            agent_instance=f"verify-{socket.gethostname()}"[:128], agent_version=__version__,
            capabilities=adapter.capabilities(), free_slots=0))
        checks.append(Check("control plane", True, cfg.control_plane))
    except PlatformError as exc:
        checks.append(Check("control plane", False, f"{exc.code.value}: {exc}"))
    except httpx.HTTPError as exc:
        checks.append(Check("control plane", False, f"unreachable: {exc}"))

    if cfg.adapter == "kubernetes":
        missing = adapter.missing_permissions()
        checks.append(Check("scheduler permissions", not missing,
                            "missing: " + ", ".join(missing) if missing else
                            f"namespace {cfg.kubernetes.namespace}"))
    else:
        root = Path(cfg.local_pool.work_root)
        ok = root.is_dir() and os.access(root, os.W_OK)
        checks.append(Check("scheduler permissions", ok, f"work root {root}"))

    store = open_store(cfg.store)
    key = f"{VERIFY_PREFIX}{uuid.uuid4().hex}/agent.json"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            probe = Path(tmp) / "agent.json"
            probe.write_text(json.dumps({"agent": __version__}), encoding="utf-8")
            store.put_file(key, probe, if_absent=True)
            stat = store.stat(key)
            ok = stat == (probe.stat().st_size, sha256_file(probe))
        store.delete(key)
        checks.append(Check("storage", ok and not store.exists(key), cfg.store))
    except Exception as exc:  # any backend error is the finding
        checks.append(Check("storage", False, f"{type(exc).__name__}: {exc}"))

    if image:
        resolved = image
        if cfg.image_trust is not None:
            try:
                resolved = ImageVerifier(cfg.image_trust).resolve(image)
                checks.append(Check("image signature", True, resolved))
            except UntrustedImage as exc:
                checks.append(Check("image signature", False, str(exc)))
                return checks
        elif not cfg.allow_unsigned_images:
            checks.append(Check("image signature", False,
                                "no pinned image keys; this agent would refuse every image"))
            return checks
        if cfg.adapter == "kubernetes":
            runner_key = f"{VERIFY_PREFIX}{uuid.uuid4().hex}/runner.json"
            ok, tail = adapter.run_probe(resolved, cfg.store, runner_key)
            wrote = ok and store.exists(runner_key)
            checks.append(Check("runner pod", wrote, tail[-500:]))
            if wrote:
                store.delete(runner_key)
    return checks


def report(checks: list[Check]) -> str:
    return json.dumps({"ok": all(c.ok for c in checks),
                       "checks": [asdict(c) for c in checks]}, indent=2)


def dry_run(cfg: AgentConfig) -> dict:
    """Every call this agent makes, by system, without making any."""
    from .adapters.kubernetes import REQUIRED_PERMISSIONS
    from .agent import Agent

    calls = {
        "control_plane": {"url": cfg.control_plane, "calls": [
            "POST /v1/agent/poll", "POST /v1/agent/heartbeat", "POST /v1/agent/result",
            "POST /v1/agent/grants", "POST /v1/agent/evaluations/lease",
            "POST /v1/agent/evaluations/complete"]},
        "storage": {"store": cfg.store, "prefixes": list(Agent.ALLOWED_PREFIXES),
                    "calls": ["put (create-if-absent)", "get", "stat", "delete",
                              "presign GET and PUT"]},
        "egress_policy": cfg.egress.value,
    }
    if cfg.adapter == "kubernetes":
        calls["scheduler"] = {
            "adapter": "kubernetes", "namespace": cfg.kubernetes.namespace,
            "calls": [f"{verb} {resource}{'/' + sub if sub else ''}"
                      for verb, _, resource, sub in REQUIRED_PERMISSIONS]}
    else:
        calls["scheduler"] = {"adapter": "local-pool", "work_root": cfg.local_pool.work_root,
                              "calls": ["start runner processes", "signal them to stop"]}
    if cfg.image_trust is not None:
        calls["image_trust"] = {"cosign": f"{cfg.image_trust.cosign} verify --key ...",
                                "keys": cfg.image_trust.keys, "mirror": cfg.image_trust.mirror}
    return calls
