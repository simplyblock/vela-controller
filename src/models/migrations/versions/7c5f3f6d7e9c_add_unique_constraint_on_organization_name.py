"""Add unique constraint on organization.name

Revision ID: 7c5f3f6d7e9c
Revises: f4f677e4e9b9
Create Date: 2026-02-02 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "7c5f3f6d7e9c"
down_revision: Union[str, Sequence[str], None] = "f4f677e4e9b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_unique_constraint("organization_name_key", "organization", ["name"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("organization_name_key", "organization", type_="unique")
