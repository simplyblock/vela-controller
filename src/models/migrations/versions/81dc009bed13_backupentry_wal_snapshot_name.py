"""backupentry_wal_snapshot_name

Revision ID: 81dc009bed13
Revises: e7c19a2b7f11
Create Date: 2026-03-05 11:36:53.086668

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '81dc009bed13'
down_revision: Union[str, Sequence[str], None] = 'e7c19a2b7f11'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('backupentry', sa.Column('wal_snapshot_name', sa.String(length=255), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('backupentry', 'wal_snapshot_name')
