"""The agent's configuration file, its dry run, and the install's Role matching the adapter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from conftest import ROOT
from hydrocouple_platform.agent.config import AgentConfig

KUSTOMIZE = ROOT / "deploy" / "kustomize"


def test_each_adapter_needs_its_settings():
    with pytest.raises(ValueError, match="kubernetes"):
        AgentConfig(control_plane="https://p", store="s3://b/p", adapter="kubernetes")
    cfg = AgentConfig(control_plane="https://p", store="file:///s", adapter="local-pool",
                      local_pool={"work_root": "/w"})
    assert cfg.local_pool.slots == 1 and cfg.image_trust is None
    with pytest.raises(ValueError):  # unknown keys are refused, not ignored
        AgentConfig(control_plane="https://p", store="file:///s", adapter="local-pool",
                    local_pool={"work_root": "/w"}, slot=4)


@pytest.mark.parametrize("overlay", ["eks", "aks", "gke", "onprem"])
def test_every_overlays_agent_json_is_a_valid_configuration(overlay):
    cfg = AgentConfig.load(KUSTOMIZE / "overlays" / overlay / "agent.json")
    assert cfg.adapter == "kubernetes" and cfg.kubernetes.namespace == "hydrocouple"
    assert cfg.image_trust.keys == ["/etc/hydrocouple/keys/release.pub"]
    assert cfg.gateway.port == 8081 and cfg.kubernetes.queue_name == "hydrocouple"


def _role_allows(rules, verb, group, resource, subresource):
    name = f"{resource}/{subresource}" if subresource else resource
    return any(group in r["apiGroups"] and name in r["resources"] and verb in r["verbs"]
               for r in rules)


def test_the_installs_role_grants_exactly_what_the_adapter_checks_for():
    yaml = pytest.importorskip("yaml")
    from hydrocouple_platform.agent.adapters.kubernetes import REQUIRED_PERMISSIONS

    docs = list(yaml.safe_load_all((KUSTOMIZE / "base" / "rbac.yaml").read_text()))
    (role,) = [d for d in docs if d["kind"] == "Role"]
    for verb, group, resource, sub in REQUIRED_PERMISSIONS:
        assert _role_allows(role["rules"], verb, group, resource, sub), (verb, resource, sub)
    assert all(d["kind"] != "ClusterRole" for d in docs)  # nothing cluster-wide


def test_the_dry_run_names_every_call_without_making_one(artifact_dir, capsys):
    from hydrocouple_platform.cli import main

    config = json.loads((KUSTOMIZE / "overlays" / "eks" / "agent.json").read_text())
    path = artifact_dir / "agent.json"
    path.write_text(json.dumps(config))
    assert main(["agent", "--config", str(path), "--dry-run"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert "POST /v1/agent/poll" in plan["control_plane"]["calls"]
    assert "create jobs" in plan["scheduler"]["calls"] and "get pods/log" in \
        plan["scheduler"]["calls"]
    assert plan["storage"]["prefixes"] == ["revisions/", "runs/"]
    assert plan["image_trust"]["keys"] == ["/etc/hydrocouple/keys/release.pub"]


def test_the_agent_command_still_runs_a_local_pool_from_flags(monkeypatch, capsys,
                                                             artifact_dir):
    from hydrocouple_platform.cli import main

    assert main(["agent", "--control-plane", "http://p", "--store",
                 f"file://{artifact_dir}/store", "--work-root", str(artifact_dir),
                 "--dry-run"]) == 0
    assert json.loads(capsys.readouterr().out)["scheduler"]["adapter"] == "local-pool"
    monkeypatch.delenv("HCP_AGENT_CREDENTIAL", raising=False)
    with pytest.raises(SystemExit):  # running it needs the credential
        main(["agent", "--control-plane", "http://p", "--store", "file:///s",
              "--work-root", str(artifact_dir)])
    assert Path(artifact_dir).exists()
