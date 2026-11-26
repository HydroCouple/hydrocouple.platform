"""AI agent sessions: own credential, actor chain, delegated budgets and human approval."""

from __future__ import annotations

import dataclasses

import pytest
from sqlalchemy import select

from hydrocouple_platform.client import PlatformClient
from hydrocouple_platform.contracts.errors import ErrorCode, PlatformError
from hydrocouple_platform.contracts.requests import (
    AgentSessionCreate,
    AgentSessionView,
    ApprovalView,
    Budget,
    TargetCreate,
)
from hydrocouple_platform.db import transaction
from hydrocouple_platform.db.models import Approval, AuditRecord
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
    AgentSessionView.model_validate({k: v for k, v in row.items() if k != "agent_session_token"})
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
    ApprovalView.model_validate(pending[0])  # the published approval-view-1 contract
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
            project_id=world.project_id, client_name="x", conversation_ref="conv-x")),
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
        me = bot.me()
        assert me["subject"] == "alice" and me["agent_session_id"] == row["agent_session_id"]
        accepted = bot.submit_run({
            "project_id": world.project_id, "revision_id": revision["revision_id"],
            "workflow": "swmm.simulation", "workflow_version": "1",
            "target_id": world.target_id}, idempotency_key="key-http-session")
        assert accepted["status"] == "PENDING_APPROVAL"
        with pytest.raises(PlatformError) as err:
            bot.resolve_approval(accepted["approval_id"], True)
        assert err.value.code == ErrorCode.FORBIDDEN


def test_a_tool_call_in_the_header_and_in_the_body_must_agree(world):
    revision = ready_revision(world)
    _, bot = agent(world)
    header = dataclasses.replace(bot, tool_call_id="toolu_header")
    with pytest.raises(PlatformError) as two:
        world.services.submit_run(
            header, world.run_request(revision["revision_id"], tool_call_id="toolu_body"),
            "key-two-calls")
    assert two.value.code == ErrorCode.VALIDATION_ERROR
    world.services.submit_run(header, world.run_request(revision["revision_id"]), "key-header")
    world.services.dispatch()
    assert world.poll().leases[0].spec.actor.tool_call_id == "toolu_header"


def test_cancellations_and_approvals_keep_the_agents_chain(world):
    revision = ready_revision(world)
    row, bot = agent(world, max_runs=1)
    first = world.services.submit_run(dataclasses.replace(bot, tool_call_id="toolu_run_1"),
                                      world.run_request(revision["revision_id"]), "key-chain-1")
    held = world.services.submit_run(dataclasses.replace(bot, tool_call_id="toolu_run_2"),
                                     world.run_request(revision["revision_id"]), "key-chain-2")
    assert held.status == "PENDING_APPROVAL"
    world.services.resolve_approval(world.admin, held.approval_id, True, "fine")
    world.services.cancel_job(dataclasses.replace(bot, tool_call_id="toolu_cancel"),
                              first.job_id)
    with transaction(world.services._factory) as s:
        assert s.get(Approval, held.approval_id).actor["tool_call_id"] == "toolu_run_2"
        decided = s.scalar(select(AuditRecord).where(AuditRecord.action == "approval.approved"))
        cancelled = s.scalar(select(AuditRecord).where(AuditRecord.action == "job.cancel"))
    # A person decides; the record keeps both who decided and whose request it was.
    assert decided.actor["human_subject"] == "alice" and decided.actor["agent_session_id"] is None
    assert decided.details["requested_by"]["tool_call_id"] == "toolu_run_2"
    assert decided.details["requested_by"]["agent_session_id"] == row["agent_session_id"]
    assert cancelled.actor["tool_call_id"] == "toolu_cancel"
    assert cancelled.actor["agent_session_id"] == row["agent_session_id"]
    assert cancelled.actor["model"] == "claude-opus-5-5"


def test_a_session_reads_its_budget_over_http_and_cannot_set_policy(
        services, artifact_dir, server, make_token):
    world = make_world(services, artifact_dir)
    revision = ready_revision(world)
    row, _ = agent(world, max_runs=3, max_concurrent_attempts=2)
    with PlatformClient(server, row["agent_session_token"]) as bot:
        bot.submit_run({
            "project_id": world.project_id, "revision_id": revision["revision_id"],
            "workflow": "swmm.simulation", "workflow_version": "1",
            "target_id": world.target_id}, idempotency_key="key-view-1")
        view = AgentSessionView.model_validate(bot.agent_session())
        assert view.used["runs"] == 1 and view.remaining["runs"] == 2
        assert view.used["concurrent_attempts"] == 1 and view.remaining["concurrent_attempts"] == 1
        assert view.remaining["evaluations"] is None  # the budget sets no limit
        with pytest.raises(PlatformError) as refused:
            bot.set_policy(world.org_id, {"require_estimate_above_evaluations": 0})
        assert refused.value.code == ErrorCode.FORBIDDEN
    with PlatformClient(server, make_token("alice")) as person:
        assert person.set_policy(world.org_id, {"require_estimate_above_evaluations": 0}) == {
            "require_estimate_above_evaluations": 0}
        with pytest.raises(PlatformError) as not_a_session:
            person.agent_session()
        assert not_a_session.value.code == ErrorCode.NOT_FOUND
        with pytest.raises(PlatformError) as needs:
            person.submit_run({
                "project_id": world.project_id, "revision_id": revision["revision_id"],
                "workflow": "swmm.simulation", "workflow_version": "1",
                "target_id": world.target_id}, idempotency_key="key-view-2")
        assert needs.value.code == ErrorCode.ESTIMATE_REQUIRED
        estimate = person.estimate({
            "project_id": world.project_id, "revision_id": revision["revision_id"],
            "workflow": "swmm.simulation", "workflow_version": "1",
            "target_id": world.target_id})
        accepted = person.submit_run({
            "project_id": world.project_id, "revision_id": revision["revision_id"],
            "workflow": "swmm.simulation", "workflow_version": "1",
            "target_id": world.target_id, "estimate_id": estimate["estimate_id"]},
            idempotency_key="key-view-3")
        assert accepted["status"] == "ACCEPTED"


def test_a_session_names_the_conversation_it_works_for(services, artifact_dir, server, make_token):
    world = make_world(services, artifact_dir)
    with PlatformClient(server, make_token("alice")) as person:
        for missing in ({}, {"conversation_ref": ""}):
            with pytest.raises(PlatformError) as refused:
                person.create_agent_session(
                    project_id=world.project_id, client_name="claude-code", **missing)
            assert refused.value.code == ErrorCode.VALIDATION_ERROR
        row = person.create_agent_session(project_id=world.project_id, client_name="claude-code",
                                          conversation_ref="conv-9")
    principal = services.session_principal(row["agent_session_token"])
    with transaction(services._factory) as s:
        assert services.actor_of(s, principal).conversation_ref == "conv-9"
