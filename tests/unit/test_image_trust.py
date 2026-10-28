"""Image trust without a registry: references, mirrors, pinned keys and the agent's refusal.

``tests/k8s/test_platform_on_kubernetes.py`` verifies real cosign signatures in a
registry, with the right key, another key and none.
"""

from __future__ import annotations

import stat
from types import SimpleNamespace

import pytest

from hydrocouple_platform.agent.imagetrust import (
    ImageTrust,
    ImageVerifier,
    UntrustedImage,
    mirrored,
)
from hydrocouple_platform.contracts.jobs import FailureClass

DIGEST = "sha256:" + "a" * 64


@pytest.mark.parametrize("image, mirror, expected", [
    (f"ghcr.io/hydrocouple/runner@{DIGEST}", None, f"ghcr.io/hydrocouple/runner@{DIGEST}"),
    (f"ghcr.io/hydrocouple/runner@{DIGEST}", "mirror.utility.local/platform/",
     f"mirror.utility.local/platform/hydrocouple/runner@{DIGEST}"),
    (f"127.0.0.1:5000/hydrocouple/runner@{DIGEST}", "registry.site/m",
     f"registry.site/m/hydrocouple/runner@{DIGEST}"),
    (f"hydrocouple/runner@{DIGEST}", "registry.site/m",
     f"registry.site/m/hydrocouple/runner@{DIGEST}"),
])
def test_a_mirror_keeps_the_repository_path_and_the_digest(image, mirror, expected):
    assert mirrored(image, mirror) == expected


@pytest.mark.parametrize("image", ["ghcr.io/hydrocouple/runner:1.0", "ghcr.io/x@sha256:abc",
                                   "", None])
def test_only_images_by_digest_are_considered(image):
    with pytest.raises(UntrustedImage):
        mirrored(image, None)


def fake_cosign(directory, accepts: str):
    """A stand-in cosign that accepts only signatures checked with ``accepts``."""
    script = directory / "cosign"
    calls = directory / "cosign.calls"
    script.write_text(f"""#!/bin/sh
echo "$@" >> {calls}
case "$*" in *"--key {accepts} "*) exit 0 ;; esac
echo "Error: no matching signatures" >&2
exit 1
""")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script, calls


def test_any_pinned_key_may_have_signed_the_image_and_verdicts_are_cached(artifact_dir):
    old, new = artifact_dir / "old.pub", artifact_dir / "new.pub"
    old.write_text("old")
    new.write_text("new")
    cosign, calls = fake_cosign(artifact_dir, accepts=str(new))
    verifier = ImageVerifier(ImageTrust(keys=[str(old), str(new)], cosign=str(cosign),
                                        mirror="mirror.site/m"))
    image = f"ghcr.io/hydrocouple/runner@{DIGEST}"
    assert verifier.resolve(image) == f"mirror.site/m/hydrocouple/runner@{DIGEST}"
    assert verifier.resolve(image) == f"mirror.site/m/hydrocouple/runner@{DIGEST}"
    lines = calls.read_text().splitlines()
    assert len(lines) == 2  # both keys tried once; the second call came from the cache
    assert all("--insecure-ignore-tlog=true" in line for line in lines)


def test_an_image_no_pinned_key_signed_is_untrusted(artifact_dir):
    key = artifact_dir / "release.pub"
    key.write_text("release")
    cosign, _ = fake_cosign(artifact_dir, accepts="/nowhere")
    verifier = ImageVerifier(ImageTrust(keys=[str(key)], cosign=str(cosign),
                                        require_transparency_log=True))
    with pytest.raises(UntrustedImage) as refused:
        verifier.resolve(f"ghcr.io/hydrocouple/runner@{DIGEST}")
    assert "not signed by a pinned key" in str(refused.value)
    assert "no matching signatures" in str(refused.value)
    with pytest.raises(ValueError):
        ImageVerifier(ImageTrust(keys=[str(artifact_dir / "missing.pub")]))


class ImageAdapter:
    """Runs images, and records what it was asked to start."""

    def __init__(self):
        self.submitted = []

    def capabilities(self):
        return SimpleNamespace(runs_images=True)

    def submit(self, lease, store_uri):
        self.submitted.append(lease)
        return lease.attempt_id


def lease_with(image):
    from datetime import datetime, timezone

    from hydrocouple_platform.contracts.agent import LeaseGrant

    return LeaseGrant.model_validate({
        "attempt_id": "att_1", "job_id": "job_1", "fencing_token": 1, "attempt_token": "t",
        "lease_expires_at": datetime.now(timezone.utc), "heartbeat_interval_seconds": 15,
        "spec": {
            "job_id": "job_1", "run_id": "run_1", "organization_id": "org_1",
            "project_id": "prj_1", "actor": {"human_subject": "alice"},
            "workload": {"workflow": "swmm.simulation", "workflow_version": "1"},
            "runtime": {"image": image},
            "inputs": {"revision_id": "rev_1", "manifest_sha256": "b" * 64,
                       "storage_prefix": "revisions/rev_1/",
                       "files": [{"path": "composition.json", "sha256": "c" * 64, "size": 1}]},
            "resources": {}, "placement": {"target_id": "tgt_1"}, "limits": {},
            "outputs": {"storage_prefix": "runs/x/", "required_artifacts": ["run.json"]}}})


def test_an_agent_without_pinned_keys_runs_no_images_unless_told_to(artifact_dir):
    from hydrocouple_platform.agent import Agent
    from hydrocouple_platform.contracts.jobs import EgressPolicy

    image = f"ghcr.io/hydrocouple/runner@{DIGEST}"
    adapter = ImageAdapter()
    strict = Agent(None, adapter, None, "file:///s", EgressPolicy.SUMMARIES)
    strict._start(lease_with(image))
    assert adapter.submitted == []
    assert strict.held["att_1"].final.failure_class == FailureClass.UNTRUSTED_IMAGE

    development = Agent(None, adapter, None, "file:///s", EgressPolicy.SUMMARIES,
                        allow_unsigned_images=True)
    development._start(lease_with(image))
    assert [lease.spec.runtime.image for lease in adapter.submitted] == [image]

    key = artifact_dir / "release.pub"
    key.write_text("release")
    cosign, _ = fake_cosign(artifact_dir, accepts=str(key))
    adapter.submitted.clear()
    mirroring = Agent(None, adapter, None, "file:///s", EgressPolicy.SUMMARIES,
                      images=ImageVerifier(ImageTrust(keys=[str(key)], cosign=str(cosign),
                                                      mirror="mirror.site/m")))
    mirroring._start(lease_with(image))
    # The runner is started from the mirror, and its spec names the image it runs.
    assert adapter.submitted[0].spec.runtime.image == f"mirror.site/m/hydrocouple/runner@{DIGEST}"


def test_workflow_images_must_be_pinned_by_digest():
    from hydrocouple_platform.config import ControlPlaneSettings

    base = dict(database_url="postgresql://x", jwt_issuer="i", jwt_audience="a",
                jwks_file="/k", attempt_token_secret="s" * 40)
    ok = ControlPlaneSettings(**base, workflow_images={"swmm.simulation@1": f"r/x@{DIGEST}"})
    assert ok.workflow_images["swmm.simulation@1"].endswith(DIGEST)
    with pytest.raises(ValueError):
        ControlPlaneSettings(**base, workflow_images={"swmm.simulation@1": "r/x:latest"})
