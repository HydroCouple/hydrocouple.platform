"""Onboarding a target from the command line: register, then the credential works."""

from __future__ import annotations

import json

import pytest

from world import make_world

pytestmark = pytest.mark.postgres


def test_target_register_prints_a_kubernetes_secret_holding_a_working_credential(
        services, artifact_dir, server, make_token, monkeypatch, capsys):
    from hydrocouple_platform.cli import main

    world = make_world(services, artifact_dir)
    monkeypatch.setenv("HCP_TOKEN", make_token("alice"))
    assert main(["target", "register", "--control-plane", server, "--org", world.org_id,
                 "--name", "utility-eks", "--adapter", "kubernetes",
                 "--storage-uri", "s3://bucket/hydrocouple", "--egress", "metrics-only",
                 "--kubernetes-secret", "hydrocouple/hcp-agent-credential"]) == 0
    out = capsys.readouterr()
    secret = json.loads(out.out)
    assert secret["kind"] == "Secret" and secret["metadata"]["namespace"] == "hydrocouple"
    assert secret["metadata"]["name"] == "hcp-agent-credential"
    target_id = secret["metadata"]["labels"]["hydrocouple.org/target"]
    assert f"registered target {target_id}" in out.err
    target = services.agent_target(secret["stringData"]["credential"])
    assert target.id == target_id and target.egress_policy == "metrics-only"

    monkeypatch.delenv("HCP_TOKEN")
    assert main(["target", "register", "--control-plane", server, "--org", world.org_id,
                 "--name", "x", "--adapter", "kubernetes", "--storage-uri", "s3://b/p"]) == 2
