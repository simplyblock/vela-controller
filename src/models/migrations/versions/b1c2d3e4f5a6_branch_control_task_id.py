"""Add control_task_id to branch table

Revision ID: b1c2d3e4f5a6
Revises: 07822c477427
Create Date: 2026-04-01 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "b1c2d3e4f5a6"
down_revision: Union[str, Sequence[str], None] = "07822c477427"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("branch", sa.Column("control_task_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("branch", "control_task_id")
