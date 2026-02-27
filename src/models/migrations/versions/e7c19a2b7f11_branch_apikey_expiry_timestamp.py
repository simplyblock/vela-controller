"""Add branchapikey.expiry_timestamp

Revision ID: e7c19a2b7f11
Revises: dcbbed40dd5e
Create Date: 2026-02-27 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = 'e7c19a2b7f11'
down_revision: Union[str, Sequence[str], None] = 'dcbbed40dd5e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'branchapikey',
        sa.Column(
            'expiry_timestamp',
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now() + interval '10 years'"),
        ),
    )
    op.alter_column('branchapikey', 'expiry_timestamp', server_default=None)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('branchapikey', 'expiry_timestamp')
