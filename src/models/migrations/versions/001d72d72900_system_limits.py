"""system_limits

Revision ID: 001d72d72900
Revises: a35c93231794
Create Date: 2026-02-23 11:18:20.047859

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql

from ulid import ULID


# revision identifiers, used by Alembic.
revision: str = '001d72d72900'
down_revision: str | Sequence[str] | None = 'a35c93231794'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    entity_type_enum = sa.Enum("system", "org", "project", name="entitytype", create_type=False)
    resource_type_enum = sa.Enum("milli_vcpu", "ram", "iops", "database_size", "storage_size", name="resourcetype", create_type=False)
    system_entity_type = sa.type_coerce("system", entity_type_enum)
    resource_limit = sa.table(
        "resourcelimit",
        sa.column("id", sa.UUID),
        sa.column("entity_type", entity_type_enum),
        sa.column("org_id", sa.UUID),
        sa.column("project_id", sa.UUID),
        sa.column("resource", sa.String),
        sa.column("max_total", sa.BigInteger),
        sa.column("max_per_branch", sa.BigInteger),
    )
    default_system_limits = [
        ("milli_vcpu",    10 ** 5),
        ("ram",           2 ** 38),
        ("iops",          10 ** 9),
        ("database_size", 10 ** 14),
        ("storage_size",  10 ** 12),
    ]

    conn = op.get_bind()

    existing = {
        row.resource
        for row in conn.execute(
            resource_limit.select().where(resource_limit.c.entity_type == system_entity_type)
        )
    }

    for resource_type, limit in default_system_limits:
        if resource_type in existing:
            continue
        conn.execute(sa.insert(resource_limit).values(
            id=ULID().to_uuid(),
            entity_type=system_entity_type,
            org_id=None,
            project_id=None,
            resource=sa.type_coerce(resource_type, resource_type_enum),
            max_total=limit,
            max_per_branch=limit,
        )
    )



def downgrade() -> None:
    """Downgrade schema."""
    pass  # Do not delete limits, they may have been previously established
