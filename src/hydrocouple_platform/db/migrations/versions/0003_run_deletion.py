"""run deletion: the deletions table, and the runs' tombstone columns

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '0003'
down_revision = '0002'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('deletions',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('run_id', sa.String(length=40), nullable=False),
    sa.Column('target_id', sa.String(length=40), nullable=False),
    sa.Column('study_id', sa.String(length=40), nullable=True),
    sa.Column('state', sa.String(length=30), nullable=False),
    sa.Column('prefixes', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('passes', sa.Integer(), nullable=False),
    sa.Column('objects_deleted', sa.BigInteger(), nullable=False),
    sa.Column('reason', sa.Text(), nullable=True),
    sa.Column('not_before', sa.DateTime(timezone=True), nullable=True),
    sa.Column('delivered_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('requested_by', sa.String(length=40), nullable=False),
    sa.Column('actor', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('requested_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state in ('REQUESTED','WAITING_FOR_WRITERS','BLOCKED','ERASING','VERIFYING','ERASED')", name='deletion_state_known'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['requested_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ),
    sa.ForeignKeyConstraint(['study_id'], ['studies.id'], name='deletions_study_fk'),
    sa.ForeignKeyConstraint(['target_id'], ['compute_targets.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('run_id')
    )
    op.create_index('deletions_target', 'deletions', ['target_id', 'state'], unique=False)
    op.create_index('deletions_work', 'deletions', ['state', 'updated_at'], unique=False)
    op.create_index(op.f('ix_deletions_organization_id'), 'deletions', ['organization_id'], unique=False)
    op.add_column('runs', sa.Column('deleting_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('runs', sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('attempts', sa.Column('released_at', sa.DateTime(timezone=True), nullable=True))
    # Attempts that ended before this column existed are taken as released.
    op.execute("UPDATE attempts SET released_at = coalesce(finished_at, started_at) "
               "WHERE state IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','LOST')")
    op.create_index('attempts_unreleased', 'attempts', ['target_id', 'agent_instance'],
                    unique=False, postgresql_where=sa.text('released_at IS NULL'))


def downgrade() -> None:
    op.drop_index('attempts_unreleased', table_name='attempts',
                  postgresql_where=sa.text('released_at IS NULL'))
    op.drop_column('attempts', 'released_at')
    op.drop_column('runs', 'deleted_at')
    op.drop_column('runs', 'deleting_at')
    op.drop_index(op.f('ix_deletions_organization_id'), table_name='deletions')
    op.drop_index('deletions_work', table_name='deletions')
    op.drop_index('deletions_target', table_name='deletions')
    op.drop_table('deletions')
