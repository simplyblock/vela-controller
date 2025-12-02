"""backup_storage_snapshot

Revision ID: f0c1d5d1b7a6
Revises: c53b3f8367e6
Create Date: 2025-12-01 15:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f0c1d5d1b7a6"
down_revision: Union[str, Sequence[str], None] = "c53b3f8367e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "backupentry",
        sa.Column("storage_snapshot_name", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "backupentry",
        sa.Column("storage_snapshot_namespace", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "backupentry",
        sa.Column("storage_snapshot_content_name", sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("backupentry", "storage_snapshot_content_name")
    op.drop_column("backupentry", "storage_snapshot_namespace")
    op.drop_column("backupentry", "storage_snapshot_name")
