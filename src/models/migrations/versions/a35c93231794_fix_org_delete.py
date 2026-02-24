"""fix_org_delete

Revision ID: a35c93231794
Revises: 06bbdb4463ec
Create Date: 2026-02-24 21:31:07.816901

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = 'a35c93231794'
down_revision: Union[str, Sequence[str], None] = '06bbdb4463ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # NULL out org_id on all project-level limits; the organization is
    # accessible transitively via the project relationship.
    op.execute("""
        UPDATE resourcelimit
        SET org_id = NULL
        WHERE project_id IS NOT NULL
    """)

    # Replace the old project uniqueness index (which included org_id and
    # required org_id IS NOT NULL) with one keyed only on project_id.
    op.drop_index(
        "uq_limit_project",
        table_name="resourcelimit",
        postgresql_where=sa.text("org_id IS NOT NULL AND project_id IS NOT NULL"),
    )
    op.create_index(
        "uq_limit_project",
        "resourcelimit",
        ["entity_type", "resource", "project_id"],
        unique=True,
        postgresql_where=sa.text("project_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "uq_limit_project",
        table_name="resourcelimit",
        postgresql_where=sa.text("project_id IS NOT NULL"),
    )
    op.create_index(
        "uq_limit_project",
        "resourcelimit",
        ["entity_type", "resource", "org_id", "project_id"],
        unique=True,
        postgresql_where=sa.text("org_id IS NOT NULL AND project_id IS NOT NULL"),
    )
    # Re-populate org_id from the project's organization_id.
    op.execute("""
        UPDATE resourcelimit rl
        SET org_id = p.organization_id
        FROM project p
        WHERE rl.project_id = p.id
    """)
