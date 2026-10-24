"""Tenant isolation, roles, target revocation and the HTTP surface."""

from __future__ import annotations

import httpx
import pytest

from conftest import ISSUER
from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.agent import Heartbeat
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.states import AttemptState
from world import make_world, ready_revision

pytestmark = pytest.mark.postgres


@pytest.fixture
def two_orgs(services, artifact_dir):
    a = make_world(services, artifact_dir / "a", org_name="Utility A", admin="alice")
    b = make_world(services, artifact_dir / "b", org_name="Utility B", admin="bob")
    return a, b


def test_other_organizations_see_nothing(two_orgs):
    a, b = two_orgs
    revision = ready_revision(a)
    accepted = a.services.submit_run(a.admin, a.run_request(revision["revision_id"]), "key-iso-1")
    for call in (
        lambda: b.services.get_job(b.admin, accepted.job_id),
        lambda: b.services.get_results(b.admin, accepted.run_id),
        lambda: b.services.cancel_job(b.admin, accepted.job_id),
        lambda: b.services.get_revision(b.admin, revision["revision_id"]),
        lambda: b.services.list_jobs(b.admin, a.project_id),
        lambda: b.services.submit_run(b.admin, a.run_request(revision["revision_id"]), "key-iso-2"),
    ):
        with pytest.raises(PlatformError) as err:
            call()
        assert err.value.code == ErrorCode.NOT_FOUND


def test_viewers_read_but_cannot_submit_or_cancel(two_orgs):
    a, _ = two_orgs
    revision = ready_revision(a)
    accepted = a.services.submit_run(a.admin, a.run_request(revision["revision_id"]), "key-view-1")
    a.services.add_member(a.admin, a.org_id, ISSUER, "victor", "viewer")
    viewer = a.principal("victor")
    assert a.services.get_job(viewer, accepted.job_id).state == "ACCEPTED"
    for call in (lambda: a.services.cancel_job(viewer, accepted.job_id),
                 lambda: a.services.submit_run(viewer, a.run_request(revision["revision_id"]),
                                               "key-view-2")):
        with pytest.raises(PlatformError) as err:
            call()
        assert err.value.code == ErrorCode.FORBIDDEN


def test_an_agent_cannot_report_for_another_targets_attempt(two_orgs):
    a, b = two_orgs
    revision = ready_revision(a)
    a.services.submit_run(a.admin, a.run_request(revision["revision_id"]), "key-cross-1")
    a.services.dispatch()
    lease = a.poll().leases[0]
    with pytest.raises(PlatformError):
        b.services.agent_heartbeat(b.target_id, lease.attempt_token, Heartbeat(
            attempt_id=lease.attempt_id, fencing_token=lease.fencing_token,
            state=AttemptState.RUNNING))


def test_revoked_targets_stop_claiming_work(two_orgs):
    a, _ = two_orgs
    a.services.revoke_target(a.admin, a.target_id)
    with pytest.raises(PlatformError) as err:
        a.services.agent_target(a.credential)
    assert err.value.code == ErrorCode.TARGET_REVOKED


def test_http_authentication_and_error_bodies(services, artifact_dir, server, make_token):
    world = make_world(services, artifact_dir)
    with PlatformClient(server, make_token("alice")) as client:
        assert client.me()["roles"] == {world.org_id: "admin"}
        with pytest.raises(PlatformError) as err:
            client.job("job_doesnotexist")
        assert err.value.code == ErrorCode.NOT_FOUND
    for token in (make_token("alice", audience="someone-else"), make_token("alice", expires_in=-5),
                  make_token("alice", issuer="https://evil.example"), world.credential):
        response = httpx.get(server + "/v1/me", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code in (401, 403)
        assert response.json()["code"] in ("UNAUTHENTICATED", "FORBIDDEN")
    response = httpx.post(server + "/v1/agent/poll", json={},
                          headers={"Authorization": f"Bearer {make_token('alice')}"})
    assert response.status_code == 403
    response = httpx.post(server + "/v1/runs", json={},
                          headers={"Authorization": f"Bearer {make_token('alice')}"})
    assert response.status_code == 422 and response.json()["code"] == "VALIDATION_ERROR"


def test_settings_refuse_to_start_without_a_verifier(database_url):
    from pydantic import ValidationError

    from hydrocouple_platform.config import ControlPlaneSettings

    with pytest.raises(ValidationError, match="exactly one"):
        ControlPlaneSettings(database_url=database_url, jwt_issuer=ISSUER, jwt_audience="x",
                             attempt_token_secret="s" * 40)
