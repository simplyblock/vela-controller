"""Add branch.status_updated_at

Revision ID: f4f677e4e9b9
Revises: 6ca4a821c6a6
Create Date: 2026-03-06 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = 'f4f677e4e9b9'
down_revision: Union[str, Sequence[str], None] = '6ca4a821c6a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('branch', sa.Column('status_updated_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('branch', 'status_updated_at')
