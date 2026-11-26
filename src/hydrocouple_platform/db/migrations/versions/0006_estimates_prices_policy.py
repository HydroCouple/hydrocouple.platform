"""estimates, prices and policy: estimates a submission can name, target price tables,
an organization's submission policy, and an agent session's committed estimated spend

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '0006'
down_revision = '0005'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('organizations', sa.Column(
        'policy', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('compute_targets', sa.Column(
        'price_table', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('agent_sessions', sa.Column(
        'estimated_spend', sa.Float(), server_default='0', nullable=False))
    op.create_table('estimates',
    sa.Column('id', sa.String(length=40), nullable=False),
    sa.Column('organization_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', sa.String(length=40), nullable=False),
    sa.Column('user_id', sa.String(length=40), nullable=False),
    sa.Column('agent_session_id', sa.String(length=40), nullable=True),
    sa.Column('kind', sa.String(length=10), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('evaluations', sa.Integer(), nullable=False),
    sa.Column('estimated_spend', sa.Float(), nullable=True),
    sa.Column('requires_approval', sa.Boolean(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("kind in ('run','study')", name='estimate_kind_known'),
    sa.ForeignKeyConstraint(['agent_session_id'], ['agent_sessions.id'], ),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_estimates_organization_id'), 'estimates', ['organization_id'],
                    unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_estimates_organization_id'), table_name='estimates')
    op.drop_table('estimates')
    op.drop_column('agent_sessions', 'estimated_spend')
    op.drop_column('compute_targets', 'price_table')
    op.drop_column('organizations', 'policy')
