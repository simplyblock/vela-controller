"""fix-system-limits

Revision ID: e8f3a2d51c9b
Revises: 9bebcc605033
Create Date: 2026-03-20 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql
from alembic import op
from ulid import ULID

# revision identifiers, used by Alembic.
revision: str = "e8f3a2d51c9b"
down_revision: Union[str, Sequence[str], None] = "9bebcc605033"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    resource_type_enum = sa.Enum(
        "milli_vcpu", "ram", "iops", "database_size", "storage_size",
        name="resourcetype",
        create_type=False,
    )

    # Create the organizationlimitdefault table
    op.create_table(
        "organizationlimitdefault",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("resource", resource_type_enum, nullable=False),
        sa.Column("max_total", sa.BigInteger(), nullable=False),
        sa.Column("max_per_branch", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("uq_org_limit_default_resource", "organizationlimitdefault", ["resource"], unique=True)

    # Migrate system limits → OrganizationLimitDefault
    conn = op.get_bind()
    system_rows = conn.execute(
        sa.text("SELECT resource, max_total, max_per_branch FROM resourcelimit WHERE entity_type = 'system'")
    ).fetchall()

    org_limit_default = sa.table(
        "organizationlimitdefault",
        sa.column("id", sa.UUID),
        sa.column("resource", resource_type_enum),
        sa.column("max_total", sa.BigInteger),
        sa.column("max_per_branch", sa.BigInteger),
    )
    for resource, max_total, max_per_branch in system_rows:
        conn.execute(
            sa.insert(org_limit_default).values(
                id=ULID().to_uuid(),
                resource=resource,
                max_total=max_total,
                max_per_branch=max_per_branch,
            )
        )

    conn.execute(sa.text("DELETE FROM resourcelimit WHERE entity_type = 'system'"))


def downgrade() -> None:
    """Downgrade schema."""
    # Data migration is not trivially reversible; just drop the table.
    op.drop_index("uq_org_limit_default_resource", table_name="organizationlimitdefault")
    op.drop_table("organizationlimitdefault")
