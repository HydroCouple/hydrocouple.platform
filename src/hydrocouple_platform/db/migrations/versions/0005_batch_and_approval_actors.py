"""batch and approval actors: who proposed a batch, and whose request an approval holds

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = '0005'
down_revision = '0004'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('study_batches', sa.Column(
        'actor', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('approvals', sa.Column(
        'actor', postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade() -> None:
    op.drop_column('approvals', 'actor')
    op.drop_column('study_batches', 'actor')
