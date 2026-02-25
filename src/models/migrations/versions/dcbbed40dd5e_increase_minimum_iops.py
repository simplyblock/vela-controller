"""increase-minimum-iops

Revision ID: dcbbed40dd5e
Revises: 001d72d72900
Create Date: 2026-02-25 17:17:25.022251

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = 'dcbbed40dd5e'
down_revision: Union[str, Sequence[str], None] = '001d72d72900'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


IOPS_MIN = 1000


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        f"UPDATE branch SET iops = {IOPS_MIN} WHERE iops < {IOPS_MIN}"
    )
    op.execute(
        f"UPDATE branchprovisioning SET amount = {IOPS_MIN} WHERE resource = 'iops' AND amount < {IOPS_MIN}"
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
