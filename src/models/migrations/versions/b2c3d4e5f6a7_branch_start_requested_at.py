"""Add start_requested_at to branch

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-31 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "branch",
        sa.Column(
            "start_requested_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("branch", "start_requested_at")
