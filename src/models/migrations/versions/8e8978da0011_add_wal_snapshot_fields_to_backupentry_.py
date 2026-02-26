"""Add WAL snapshot fields to BackupEntry table

Revision ID: 8e8978da0011
Revises: dcbbed40dd5e
Create Date: 2026-02-26 09:48:07.249057

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = '8e8978da0011'
down_revision: Union[str, Sequence[str], None] = 'dcbbed40dd5e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('backupentry', sa.Column('wal_snapshot_uuid', sa.String(length=64), nullable=True))
    op.add_column('backupentry', sa.Column('wal_snapshot_name', sa.String(length=255), nullable=True))
    op.add_column('backupentry', sa.Column('wal_snapshot_namespace', sa.String(length=255), nullable=True))
    op.add_column('backupentry', sa.Column('wal_snapshot_content_name', sa.String(length=255), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('backupentry', 'wal_snapshot_content_name')
    op.drop_column('backupentry', 'wal_snapshot_namespace')
    op.drop_column('backupentry', 'wal_snapshot_name')
    op.drop_column('backupentry', 'wal_snapshot_uuid')
