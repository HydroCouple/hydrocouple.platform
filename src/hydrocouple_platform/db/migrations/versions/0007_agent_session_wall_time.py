"""agent session wall time: when the reconciler ended a session's work at its wall time

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = '0007'
down_revision = '0006'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('agent_sessions', sa.Column(
        'work_ended_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column('agent_sessions', 'work_ended_at')
