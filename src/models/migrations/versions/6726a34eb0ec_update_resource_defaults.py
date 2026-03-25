"""update-resource-defaults

Revision ID: 6726a34eb0ec
Revises: ad471311850e
Create Date: 2026-03-25 14:27:12.714577

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = '6726a34eb0ec'
down_revision: Union[str, Sequence[str], None] = 'ad471311850e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_GIB = 1024 ** 3
_GB = 1000 ** 3


def upgrade() -> None:
    """Upgrade schema."""
    conn = op.get_bind()
    for resource, value in {
        "milli_vcpu":    10_000,
        "ram":           20 * _GIB,
        "iops":          100_000,
        "database_size": 100 * _GB,
        "storage_size":  10 * _GB,
    }.items():
        conn.execute(
            sa.text(
                "UPDATE organizationlimitdefault "
                "SET max_total = :val, max_per_branch = :val "
                "WHERE resource = :resource"
            ),
            {"val": value, "resource": resource},
        )


def downgrade() -> None:
    """Downgrade schema."""
    pass  # Previous values not stored; cannot trivially reverse
