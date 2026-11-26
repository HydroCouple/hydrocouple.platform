"""schedules: a digital twin's cycles, each submission of a cycle, and setpoint proposals

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-20
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '0004'
down_revision = '0003'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('schedules',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('session_id', sa.String(length=40), nullable=True),
    sa.Column('revision_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('workflow', sa.String(length=120), nullable=False),
    sa.Column('workflow_version', sa.String(length=20), nullable=False),
    sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('first_cycle_end', sa.DateTime(timezone=False), nullable=False),
    sa.Column('cycle_minutes', sa.Float(), nullable=False),
    sa.Column('cycles', sa.Integer(), nullable=True),
    sa.Column('pace', sa.String(length=20), nullable=False),
    sa.Column('lag_seconds', sa.Float(), nullable=False),
    sa.Column('resume_from', sa.String(length=40), nullable=True),
    sa.Column('await_decisions', sa.Boolean(), nullable=False),
    sa.Column('profile', sa.String(length=40), nullable=True),
    sa.Column('retry_budget', sa.Integer(), nullable=True),
    sa.Column('estimated_runtime_seconds', sa.Float(), nullable=True),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('next_cycle', sa.Integer(), nullable=False),
    sa.Column('tries', sa.Integer(), nullable=False),
    sa.Column('current_run_id', sa.String(length=40), nullable=True),
    sa.Column('last_run_id', sa.String(length=40), nullable=True),
    sa.Column('pause_reason', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('idempotency_key', sa.String(length=200), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('actor', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('agent_session_id', sa.String(length=40), nullable=True),
    sa.Column('created_by', sa.String(length=40), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state in ('ACTIVE','PAUSED','COMPLETED')", name='schedule_state_known'),
    sa.CheckConstraint("pace in ('replay','wall_clock')", name='schedule_pace_known'),
    sa.ForeignKeyConstraint(['agent_session_id'], ['agent_sessions.id'], ),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['current_run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['last_run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['resume_from'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['revision_id'], ['revisions.id'], ),
    sa.ForeignKeyConstraint(['session_id'], ['platform_sessions.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'idempotency_key', name='schedules_idempotency')
    )
    op.create_index('schedules_work', 'schedules', ['state', 'updated_at'], unique=False)
    op.create_index(op.f('ix_schedules_organization_id'), 'schedules', ['organization_id'],
                    unique=False)
    op.create_index(op.f('ix_schedules_project_id'), 'schedules', ['project_id'], unique=False)
    op.create_table('schedule_cycles',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('schedule_id', sa.String(length=40), nullable=False),
    sa.Column('number', sa.Integer(), nullable=False),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('cycle_end', sa.DateTime(timezone=False), nullable=False),
    sa.Column('run_id', sa.String(length=40), nullable=False),
    sa.Column('job_id', sa.String(length=40), nullable=False),
    sa.Column('resumed_from', sa.String(length=40), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ),
    sa.ForeignKeyConstraint(['resumed_from'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['schedule_id'], ['schedules.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('run_id'),
    sa.UniqueConstraint('schedule_id', 'number', 'attempt')
    )
    op.create_index(op.f('ix_schedule_cycles_schedule_id'), 'schedule_cycles', ['schedule_id'],
                    unique=False)
    op.create_table('setpoint_proposals',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('schedule_id', sa.String(length=40), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('link', sa.String(length=200), nullable=False),
    sa.Column('property', sa.String(length=80), nullable=False),
    sa.Column('value', sa.Float(), nullable=False),
    sa.Column('effective_from', sa.DateTime(timezone=False), nullable=False),
    sa.Column('issued_at', sa.DateTime(timezone=False), nullable=True),
    sa.Column('run_id', sa.String(length=40), nullable=True),
    sa.Column('proposed_by', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('note', sa.Text(), nullable=True),
    sa.Column('decided_by', sa.String(length=40), nullable=True),
    sa.Column('decided_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('decision_note', sa.Text(), nullable=True),
    sa.Column('applied_run_id', sa.String(length=40), nullable=True),
    sa.Column('applied_at', sa.DateTime(timezone=False), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state in ('PROPOSED','APPROVED','REJECTED','SUPERSEDED','APPLIED')",
                       name='proposal_state_known'),
    sa.CheckConstraint("state in ('PROPOSED','SUPERSEDED') or decided_by is not null",
                       name='proposal_decided_by_a_person'),
    sa.CheckConstraint("state <> 'APPLIED' or applied_run_id is not null",
                       name='proposal_applied_by_a_run'),
    sa.ForeignKeyConstraint(['applied_run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['decided_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['schedule_id'], ['schedules.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('proposals_schedule', 'setpoint_proposals',
                    ['schedule_id', 'state', 'effective_from'], unique=False)
    op.create_index(op.f('ix_setpoint_proposals_organization_id'), 'setpoint_proposals',
                    ['organization_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_setpoint_proposals_organization_id'), table_name='setpoint_proposals')
    op.drop_index('proposals_schedule', table_name='setpoint_proposals')
    op.drop_table('setpoint_proposals')
    op.drop_index(op.f('ix_schedule_cycles_schedule_id'), table_name='schedule_cycles')
    op.drop_table('schedule_cycles')
    op.drop_index(op.f('ix_schedules_project_id'), table_name='schedules')
    op.drop_index(op.f('ix_schedules_organization_id'), table_name='schedules')
    op.drop_index('schedules_work', table_name='schedules')
    op.drop_table('schedules')
