"""studies and evaluator pools

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('studies',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('session_id', sa.String(length=40), nullable=True),
    sa.Column('revision_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('run_id', sa.String(length=40), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('definition', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('max_evaluations', sa.Integer(), nullable=False),
    sa.Column('pool_size', sa.Integer(), nullable=False),
    sa.Column('batches_submitted', sa.Integer(), nullable=False),
    sa.Column('evaluations_submitted', sa.Integer(), nullable=False),
    sa.Column('evaluations_completed', sa.Integer(), nullable=False),
    sa.Column('evaluations_failed', sa.Integer(), nullable=False),
    sa.Column('idempotency_key', sa.String(length=200), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('actor', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('agent_session_id', sa.String(length=40), nullable=True),
    sa.Column('created_by', sa.String(length=40), nullable=False),
    sa.Column('failure', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('terminal_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state in ('PENDING_APPROVAL','OPEN','CLOSED','COMPLETED','CANCELLED','FAILED','REJECTED')", name='study_state_known'),
    sa.CheckConstraint('evaluations_submitted <= max_evaluations', name='study_budget'),
    sa.ForeignKeyConstraint(['agent_session_id'], ['agent_sessions.id'], ),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['revision_id'], ['revisions.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['session_id'], ['platform_sessions.id'], ),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'idempotency_key', name='studies_idempotency'),
    sa.UniqueConstraint('run_id')
    )
    op.create_index(op.f('ix_studies_organization_id'), 'studies', ['organization_id'], unique=False)
    op.create_index(op.f('ix_studies_project_id'), 'studies', ['project_id'], unique=False)
    op.create_table('study_batches',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('study_id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('sequence', sa.Integer(), nullable=False),
    sa.Column('offset', sa.Integer(), nullable=False),
    sa.Column('size', sa.Integer(), nullable=False),
    sa.Column('idempotency_key', sa.String(length=200), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('candidates', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('completed', sa.Integer(), nullable=False),
    sa.Column('results', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state in ('QUEUED','DONE','CANCELLED','FAILED')", name='batch_state_known'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['study_id'], ['studies.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('study_id', 'idempotency_key', name='batches_idempotency'),
    sa.UniqueConstraint('study_id', 'sequence', name='batches_sequence')
    )
    op.create_index(op.f('ix_study_batches_study_id'), 'study_batches', ['study_id'], unique=False)
    op.create_table('evaluation_chunks',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('study_id', sa.String(length=40), nullable=False),
    sa.Column('batch_id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('batch_sequence', sa.Integer(), nullable=False),
    sa.Column('start', sa.Integer(), nullable=False),
    sa.Column('stop', sa.Integer(), nullable=False),
    sa.Column('state', sa.String(length=20), nullable=False),
    sa.Column('lease_number', sa.Integer(), nullable=False),
    sa.Column('deliveries', sa.Integer(), nullable=False),
    sa.Column('holder_attempt_id', sa.String(length=40), nullable=True),
    sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('results', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('object_key', sa.Text(), nullable=True),
    sa.Column('object_sha256', sa.String(length=64), nullable=True),
    sa.Column('object_size', sa.BigInteger(), nullable=True),
    sa.Column('completed_attempt_id', sa.String(length=40), nullable=True),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state in ('PENDING','LEASED','DONE','CANCELLED')", name='chunk_state_known'),
    sa.CheckConstraint('start >= 0 and stop > start', name='chunk_range'),
    sa.ForeignKeyConstraint(['batch_id'], ['study_batches.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['study_id'], ['studies.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('chunks_expiry', 'evaluation_chunks', ['state', 'lease_expires_at'], unique=False)
    op.create_index('chunks_holder', 'evaluation_chunks', ['holder_attempt_id'], unique=False)
    op.create_index('chunks_queue', 'evaluation_chunks', ['study_id', 'state', 'batch_sequence', 'start'], unique=False)
    op.create_index(op.f('ix_evaluation_chunks_batch_id'), 'evaluation_chunks', ['batch_id'], unique=False)
    op.add_column('approvals', sa.Column('study_id', sa.String(length=40), nullable=True))
    op.alter_column('approvals', 'job_id',
               existing_type=sa.VARCHAR(length=40),
               nullable=True)
    op.create_foreign_key('approvals_study_fk', 'approvals', 'studies', ['study_id'], ['id'])
    op.add_column('jobs', sa.Column('study_id', sa.String(length=40), nullable=True))
    op.add_column('jobs', sa.Column('retired_at', sa.DateTime(timezone=True), nullable=True))
    op.create_index(op.f('ix_jobs_study_id'), 'jobs', ['study_id'], unique=False)
    op.create_foreign_key('jobs_study_fk', 'jobs', 'studies', ['study_id'], ['id'])


def downgrade() -> None:
    op.drop_constraint('jobs_study_fk', 'jobs', type_='foreignkey')
    op.drop_index(op.f('ix_jobs_study_id'), table_name='jobs')
    op.drop_column('jobs', 'retired_at')
    op.drop_column('jobs', 'study_id')
    op.drop_constraint('approvals_study_fk', 'approvals', type_='foreignkey')
    op.alter_column('approvals', 'job_id',
               existing_type=sa.VARCHAR(length=40),
               nullable=False)
    op.drop_column('approvals', 'study_id')
    op.drop_index(op.f('ix_evaluation_chunks_batch_id'), table_name='evaluation_chunks')
    op.drop_index('chunks_queue', table_name='evaluation_chunks')
    op.drop_index('chunks_holder', table_name='evaluation_chunks')
    op.drop_index('chunks_expiry', table_name='evaluation_chunks')
    op.drop_table('evaluation_chunks')
    op.drop_index(op.f('ix_study_batches_study_id'), table_name='study_batches')
    op.drop_table('study_batches')
    op.drop_index(op.f('ix_studies_project_id'), table_name='studies')
    op.drop_index(op.f('ix_studies_organization_id'), table_name='studies')
    op.drop_table('studies')
