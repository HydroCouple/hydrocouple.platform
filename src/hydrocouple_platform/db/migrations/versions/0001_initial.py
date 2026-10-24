"""initial control-plane schema

Revision ID: 0001
Revises: (none)
Create Date: 2026-10-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('audit',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=True),
    sa.Column('actor', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('action', sa.String(length=80), nullable=False),
    sa.Column('object_type', sa.String(length=40), nullable=False),
    sa.Column('object_id', sa.String(length=40), nullable=False),
    sa.Column('details', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_audit_organization_id'), 'audit', ['organization_id'], unique=False)
    op.create_table('events',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('event_id', sa.String(length=64), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('job_id', sa.String(length=40), nullable=False),
    sa.Column('attempt_id', sa.String(length=40), nullable=False),
    sa.Column('sequence', sa.Integer(), nullable=False),
    sa.Column('kind', sa.String(length=20), nullable=False),
    sa.Column('phase', sa.String(length=60), nullable=True),
    sa.Column('completed', sa.Float(), nullable=True),
    sa.Column('total', sa.Float(), nullable=True),
    sa.Column('unit', sa.String(length=40), nullable=True),
    sa.Column('message', sa.Text(), nullable=True),
    sa.Column('agent_clock_seconds', sa.Float(), nullable=True),
    sa.Column('occurred_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('received_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('attempt_id', 'event_id', name='events_dedupe')
    )
    op.create_index(op.f('ix_events_job_id'), 'events', ['job_id'], unique=False)
    op.create_index(op.f('ix_events_organization_id'), 'events', ['organization_id'], unique=False)
    op.create_table('organizations',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('outbox',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('topic', sa.String(length=60), nullable=False),
    sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('available_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('tries', sa.Integer(), nullable=False),
    sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('last_error', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('outbox_pending', 'outbox', ['processed_at', 'available_at'], unique=False)
    op.create_table('users',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('issuer', sa.String(length=400), nullable=False),
    sa.Column('subject', sa.String(length=400), nullable=False),
    sa.Column('email', sa.String(length=400), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('issuer', 'subject')
    )
    op.create_table('compute_targets',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('name', sa.String(length=120), nullable=False),
    sa.Column('ownership_mode', sa.String(length=40), nullable=False),
    sa.Column('adapter', sa.String(length=60), nullable=False),
    sa.Column('region', sa.String(length=80), nullable=True),
    sa.Column('storage_uri', sa.Text(), nullable=False),
    sa.Column('egress_policy', sa.String(length=20), nullable=False),
    sa.Column('credential_hash', sa.String(length=64), nullable=False),
    sa.Column('capabilities', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('agent_instance', sa.String(length=128), nullable=True),
    sa.Column('agent_version', sa.String(length=40), nullable=True),
    sa.Column('last_poll_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('credential_hash'),
    sa.UniqueConstraint('organization_id', 'name')
    )
    op.create_index(op.f('ix_compute_targets_organization_id'), 'compute_targets', ['organization_id'], unique=False)
    op.create_table('memberships',
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('user_id', sa.String(length=40), nullable=False),
    sa.Column('role', sa.String(length=20), nullable=False),
    sa.CheckConstraint("role in ('admin','editor','viewer')", name='role_known'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('organization_id', 'user_id')
    )
    op.create_table('projects',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('default_target_id', sa.String(length=40), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'name')
    )
    op.create_index(op.f('ix_projects_organization_id'), 'projects', ['organization_id'], unique=False)
    op.create_table('agent_sessions',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('created_by', sa.String(length=40), nullable=False),
    sa.Column('client_name', sa.String(length=120), nullable=False),
    sa.Column('client_version', sa.String(length=60), nullable=True),
    sa.Column('model', sa.String(length=120), nullable=True),
    sa.Column('conversation_ref', sa.String(length=200), nullable=True),
    sa.Column('budget', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('credential_hash', sa.String(length=64), nullable=False),
    sa.Column('runs_submitted', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('credential_hash')
    )
    op.create_index(op.f('ix_agent_sessions_organization_id'), 'agent_sessions', ['organization_id'], unique=False)
    op.create_table('platform_sessions',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('created_by', sa.String(length=40), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_platform_sessions_organization_id'), 'platform_sessions', ['organization_id'], unique=False)
    op.create_index(op.f('ix_platform_sessions_project_id'), 'platform_sessions', ['project_id'], unique=False)
    op.create_table('revisions',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('session_id', sa.String(length=40), nullable=True),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('manifest', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('manifest_sha256', sa.String(length=64), nullable=False),
    sa.Column('storage_prefix', sa.Text(), nullable=False),
    sa.Column('created_by', sa.String(length=40), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('finalized_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state in ('UPLOADING','VERIFYING','READY','FAILED')", name='state_known'),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['session_id'], ['platform_sessions.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_revisions_organization_id'), 'revisions', ['organization_id'], unique=False)
    op.create_index(op.f('ix_revisions_project_id'), 'revisions', ['project_id'], unique=False)
    op.create_table('grant_requests',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('revision_id', sa.String(length=40), nullable=True),
    sa.Column('run_id', sa.String(length=40), nullable=True),
    sa.Column('mode', sa.String(length=20), nullable=False),
    sa.Column('objects', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('grants', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('problems', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('requested_by', sa.String(length=40), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('delivered_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('fulfilled_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['requested_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['revision_id'], ['revisions.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_grant_requests_organization_id'), 'grant_requests', ['organization_id'], unique=False)
    op.create_index(op.f('ix_grant_requests_target_id'), 'grant_requests', ['target_id'], unique=False)
    op.create_table('runs',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('session_id', sa.String(length=40), nullable=True),
    sa.Column('revision_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('workflow', sa.String(length=120), nullable=False),
    sa.Column('workflow_version', sa.String(length=20), nullable=False),
    sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('job_class', sa.String(length=20), nullable=False),
    sa.Column('idempotency_key', sa.String(length=200), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('actor', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('agent_session_id', sa.String(length=40), nullable=True),
    sa.Column('created_by', sa.String(length=40), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('canonical_attempt_id', sa.String(length=40), nullable=True),
    sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('summary', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.ForeignKeyConstraint(['agent_session_id'], ['agent_sessions.id'], ),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['revision_id'], ['revisions.id'], ),
    sa.ForeignKeyConstraint(['session_id'], ['platform_sessions.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'idempotency_key', name='runs_idempotency')
    )
    op.create_index(op.f('ix_runs_organization_id'), 'runs', ['organization_id'], unique=False)
    op.create_index(op.f('ix_runs_project_id'), 'runs', ['project_id'], unique=False)
    op.create_index(op.f('ix_runs_session_id'), 'runs', ['session_id'], unique=False)
    op.create_index('runs_history', 'runs', ['organization_id', 'project_id', 'created_at'], unique=False)
    op.create_table('jobs',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('run_id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('spec', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('cancel_requested', sa.Boolean(), nullable=False),
    sa.Column('agent_unreachable', sa.Boolean(), nullable=False),
    sa.Column('current_attempt_id', sa.String(length=40), nullable=True),
    sa.Column('attempts_used', sa.Integer(), nullable=False),
    sa.Column('retry_budget', sa.Integer(), nullable=False),
    sa.Column('failure', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('progress', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('leasable_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('queue_deadline_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('terminal_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state in ('PENDING_APPROVAL','ACCEPTED','LEASABLE','LEASED','QUEUED','STARTING','RUNNING','FINALIZING','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','REJECTED')", name='job_state_known'),
    sa.CheckConstraint('attempts_used >= 0 and retry_budget >= 0', name='job_counts'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_jobs_organization_id'), 'jobs', ['organization_id'], unique=False)
    op.create_index(op.f('ix_jobs_run_id'), 'jobs', ['run_id'], unique=False)
    op.create_index('jobs_leasable', 'jobs', ['target_id', 'state', 'leasable_at'], unique=False)
    op.create_index('jobs_status', 'jobs', ['organization_id', 'state'], unique=False)
    op.create_table('approvals',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('run_id', sa.String(length=40), nullable=False),
    sa.Column('job_id', sa.String(length=40), nullable=False),
    sa.Column('agent_session_id', sa.String(length=40), nullable=True),
    sa.Column('reasons', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('requested_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('resolved_by', sa.String(length=40), nullable=True),
    sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('note', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['agent_session_id'], ['agent_sessions.id'], ),
    sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['resolved_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_approvals_organization_id'), 'approvals', ['organization_id'], unique=False)
    op.create_table('attempts',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('job_id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('number', sa.Integer(), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('agent_instance', sa.String(length=128), nullable=False),
    sa.Column('provider_reference', sa.Text(), nullable=True),
    sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('execution_deadline_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('last_heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('failure_class', sa.String(length=40), nullable=True),
    sa.Column('exit_code', sa.Integer(), nullable=True),
    sa.Column('diagnostics', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('manifest', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('manifest_sha256', sa.String(length=64), nullable=True),
    sa.Column('last_sequence', sa.Integer(), nullable=False),
    sa.CheckConstraint("state in ('LEASED','QUEUED','STARTING','RUNNING','FINALIZING','SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','LOST')", name='attempt_state_known'),
    sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('job_id', 'number', name='attempt_fence_unique')
    )
    op.create_index('attempts_deadline', 'attempts', ['state', 'execution_deadline_at'], unique=False)
    op.create_index('attempts_expiry', 'attempts', ['state', 'lease_expires_at'], unique=False)
    op.create_index(op.f('ix_attempts_job_id'), 'attempts', ['job_id'], unique=False)
    op.create_table('artifacts',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('run_id', sa.String(length=40), nullable=False),
    sa.Column('attempt_id', sa.String(length=40), nullable=False),
    sa.Column('path', sa.Text(), nullable=False),
    sa.Column('storage_key', sa.Text(), nullable=False),
    sa.Column('kind', sa.String(length=30), nullable=False),
    sa.Column('format', sa.String(length=40), nullable=False),
    sa.Column('sha256', sa.String(length=64), nullable=False),
    sa.Column('size', sa.BigInteger(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['attempt_id'], ['attempts.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('attempt_id', 'path')
    )
    op.create_index(op.f('ix_artifacts_organization_id'), 'artifacts', ['organization_id'], unique=False)
    op.create_index(op.f('ix_artifacts_run_id'), 'artifacts', ['run_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_artifacts_run_id'), table_name='artifacts')
    op.drop_index(op.f('ix_artifacts_organization_id'), table_name='artifacts')
    op.drop_table('artifacts')
    op.drop_index(op.f('ix_attempts_job_id'), table_name='attempts')
    op.drop_index('attempts_expiry', table_name='attempts')
    op.drop_index('attempts_deadline', table_name='attempts')
    op.drop_table('attempts')
    op.drop_index(op.f('ix_approvals_organization_id'), table_name='approvals')
    op.drop_table('approvals')
    op.drop_index('jobs_status', table_name='jobs')
    op.drop_index('jobs_leasable', table_name='jobs')
    op.drop_index(op.f('ix_jobs_run_id'), table_name='jobs')
    op.drop_index(op.f('ix_jobs_organization_id'), table_name='jobs')
    op.drop_table('jobs')
    op.drop_index('runs_history', table_name='runs')
    op.drop_index(op.f('ix_runs_session_id'), table_name='runs')
    op.drop_index(op.f('ix_runs_project_id'), table_name='runs')
    op.drop_index(op.f('ix_runs_organization_id'), table_name='runs')
    op.drop_table('runs')
    op.drop_index(op.f('ix_grant_requests_target_id'), table_name='grant_requests')
    op.drop_index(op.f('ix_grant_requests_organization_id'), table_name='grant_requests')
    op.drop_table('grant_requests')
    op.drop_index(op.f('ix_revisions_project_id'), table_name='revisions')
    op.drop_index(op.f('ix_revisions_organization_id'), table_name='revisions')
    op.drop_table('revisions')
    op.drop_index(op.f('ix_platform_sessions_project_id'), table_name='platform_sessions')
    op.drop_index(op.f('ix_platform_sessions_organization_id'), table_name='platform_sessions')
    op.drop_table('platform_sessions')
    op.drop_index(op.f('ix_agent_sessions_organization_id'), table_name='agent_sessions')
    op.drop_table('agent_sessions')
    op.drop_index(op.f('ix_projects_organization_id'), table_name='projects')
    op.drop_table('projects')
    op.drop_table('memberships')
    op.drop_index(op.f('ix_compute_targets_organization_id'), table_name='compute_targets')
    op.drop_table('compute_targets')
    op.drop_table('users')
    op.drop_index('outbox_pending', table_name='outbox')
    op.drop_table('outbox')
    op.drop_table('organizations')
    op.drop_index(op.f('ix_events_organization_id'), table_name='events')
    op.drop_index(op.f('ix_events_job_id'), table_name='events')
    op.drop_table('events')
    op.drop_index(op.f('ix_audit_organization_id'), table_name='audit')
    op.drop_table('audit')
