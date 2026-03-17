"""Drop branchprovisioning table

Revision ID: a1b2c3d4e5f6
Revises: f76768ab95d8
Create Date: 2026-03-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f76768ab95d8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Drop the branchprovisioning table (data now lives on Branch columns)."""
    op.drop_table("branchprovisioning")


def downgrade() -> None:
    raise NotImplementedError(
        "Downgrade not supported: branchprovisioning data was denormalized into Branch columns and cannot be restored."
    )
