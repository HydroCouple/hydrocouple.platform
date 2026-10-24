"""AI agent sessions: own credential, actor chain, delegated budgets and human approval."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.requests import AgentSessionCreate, Budget, TargetCreate
from hydrocouple_platform.db import transaction
from hydrocouple_platform.db.models import AuditRecord
from world import make_world, ready_revision

pytestmark = pytest.mark.postgres


@pytest.fixture
def world(services, artifact_dir):
    return make_world(services, artifact_dir)


def agent(world, **budget):
    """Start an agent session; returns its row and the principal its credential resolves to."""
    row = world.services.create_agent_session(world.admin, AgentSessionCreate(
        project_id=world.project_id, client_name="claude-desktop", client_version="1.4",
        model="claude-opus-5-5", conversation_ref="conv-42", budget=Budget(**budget)))
    return row, world.services.session_principal(row["agent_session_token"])


def test_actor_chain_reaches_the_job_spec_and_the_audit_log(world):
    revision = ready_revision(world)
    row, bot = agent(world)
    accepted = world.services.submit_run(
        bot, world.run_request(revision["revision_id"], tool_call_id="toolu_01"), "key-actor-1")
    world.services.dispatch()
    spec = world.poll().leases[0].spec
    assert spec.actor.human_subject == "alice"
    assert spec.actor.agent_client == "claude-desktop" and spec.actor.model == "claude-opus-5-5"
    assert spec.actor.tool_call_id == "toolu_01"
    with transaction(world.services._factory) as s:
        audit = s.scalar(select(AuditRecord).where(AuditRecord.object_id == accepted.run_id))
        assert audit.actor["agent_session_id"] == row["agent_session_id"]


def test_exceeding_the_run_budget_waits_for_a_person(world):
    revision = ready_revision(world)
    _, bot = agent(world, max_runs=1)
    first = world.services.submit_run(bot, world.run_request(revision["revision_id"]),
                                      "key-budget-1")
    assert first.status == "ACCEPTED" and first.budget_remaining == {"runs": 0}
    assert world.services.estimate(bot, world.run_request(revision["revision_id"])) \
        .requires_approval

    second = world.services.submit_run(
        bot, world.run_request(revision["revision_id"], parameters={"threads": 2}),
        "key-budget-2")
    assert second.status == "PENDING_APPROVAL" and second.approval_id
    world.services.dispatch()
    leased = world.poll(free_slots=5).leases
    assert len(leased) == 1  # only the first run is leasable

    # The agent cannot approve its own request; the person who started it can.
    with pytest.raises(PlatformError) as err:
        world.services.resolve_approval(bot, second.approval_id, True)
    assert err.value.code == ErrorCode.FORBIDDEN
    pending = world.services.list_approvals(world.admin, world.project_id)
    assert pending[0]["approval_id"] == second.approval_id
    resolved = world.services.resolve_approval(world.admin, second.approval_id, True, "ok")
    assert resolved["job_state"] == "ACCEPTED"
    world.services.dispatch()
    assert len(world.poll(free_slots=5, held=[leased[0].attempt_id]).leases) == 1


def test_agents_cannot_administer_or_spawn_sessions(world):
    _, bot = agent(world)
    for call in (
        lambda: world.services.register_target(bot, world.org_id, TargetCreate(
            name="other", ownership_mode="organization-owned", adapter="local-pool",
            storage_uri="file:///tmp/x")),
        lambda: world.services.revoke_target(bot, world.target_id),
        lambda: world.services.add_member(bot, world.org_id, "iss", "mallory", "admin"),
        lambda: world.services.create_agent_session(bot, AgentSessionCreate(
            project_id=world.project_id, client_name="x")),
    ):
        with pytest.raises(PlatformError) as err:
            call()
        assert err.value.code == ErrorCode.FORBIDDEN


def test_the_same_key_with_and_without_a_session_conflicts(world):
    revision = ready_revision(world)
    _, bot = agent(world)
    world.services.submit_run(world.admin, world.run_request(revision["revision_id"]),
                              "key-shared-1")
    with pytest.raises(PlatformError) as err:
        world.services.submit_run(bot, world.run_request(revision["revision_id"]),
                                  "key-shared-1")
    assert err.value.code == ErrorCode.CONFLICT


def test_rejected_and_expired_approvals_end_the_job(world, clock):
    revision = ready_revision(world)
    _, bot = agent(world, max_runs=0)
    a = world.services.submit_run(bot, world.run_request(revision["revision_id"]),
                                  "key-reject-1")
    world.services.resolve_approval(world.admin, a.approval_id, False, "too large")
    assert world.services.get_job(world.admin, a.job_id).state == "REJECTED"
    b = world.services.submit_run(
        bot, world.run_request(revision["revision_id"], parameters={"threads": 3}),
        "key-reject-2")
    clock.advance(world.services.settings.approval_ttl_seconds + 1)
    assert world.services.reconcile()["approvals_expired"] == 1
    assert world.services.get_job(world.admin, b.job_id).state == "REJECTED"
    with pytest.raises(PlatformError) as err:
        world.services.resolve_approval(world.admin, b.approval_id, True)
    assert err.value.code == ErrorCode.INVALID_TRANSITION


def test_scope_and_unknown_spend_need_approval(world):
    revision = ready_revision(world)
    _, bot = agent(world, allowed_targets=["tgt_elsewhere"], max_estimated_spend=50)
    est = world.services.estimate(bot, world.run_request(revision["revision_id"]))
    reasons = " | ".join(est.approval_reasons)
    assert "outside the session's allowed targets" in reasons
    assert "cost of this run is unknown" in reasons


def test_expired_and_revoked_session_credentials_stop_working(world, clock):
    row, _ = agent(world)
    world.services.revoke_agent_session(world.admin, row["agent_session_id"])
    with pytest.raises(PlatformError) as err:
        world.services.session_principal(row["agent_session_token"])
    assert err.value.code == ErrorCode.UNAUTHENTICATED
    row2, _ = agent(world)
    clock.advance(9 * 3600)
    with pytest.raises(PlatformError):
        world.services.session_principal(row2["agent_session_token"])


def test_a_session_credential_works_over_http_and_carries_the_budget(
        services, artifact_dir, server):
    world = make_world(services, artifact_dir)
    revision = ready_revision(world)
    row, _ = agent(world, max_runs=0)
    with PlatformClient(server, row["agent_session_token"]) as bot:
        assert bot.me()["subject"] == "alice"
        accepted = bot.submit_run({
            "project_id": world.project_id, "revision_id": revision["revision_id"],
            "workflow": "swmm.simulation", "workflow_version": "1",
            "target_id": world.target_id}, idempotency_key="key-http-session")
        assert accepted["status"] == "PENDING_APPROVAL"
        with pytest.raises(PlatformError) as err:
            bot.resolve_approval(accepted["approval_id"], True)
        assert err.value.code == ErrorCode.FORBIDDEN
