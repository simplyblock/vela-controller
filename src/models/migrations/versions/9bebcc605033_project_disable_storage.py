"""project-disable-storage

Revision ID: 9bebcc605033
Revises: 81dc009bed13
Create Date: 2026-03-12 19:19:02.990687

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql

from ulid import ULID


# revision identifiers, used by Alembic.
revision: str = '9bebcc605033'
down_revision: Union[str, Sequence[str], None] = '81dc009bed13'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    # Find all projects that do not yet have a 'storage_size' resource limit
    result = bind.execute(
        sa.text(
            """
            SELECT p.id
            FROM project p
            WHERE NOT EXISTS (
                SELECT 1
                FROM resourcelimit rl
                WHERE rl.project_id = p.id
                  AND rl.resource = 'storage_size'
            )
            """
        )
    )
    project_ids = [row[0] for row in result]
    for project_id in project_ids:
        new_id = ULID().to_uuid()
        bind.execute(
            sa.text(
                """
                INSERT INTO resourcelimit
                    (id, entity_type, resource, org_id, env_type, project_id, max_total, max_per_branch)
                VALUES
                    (:id, 'project', 'storage_size', NULL, NULL, :project_id, 0, 0)
                """
            ),
            {"id": new_id, "project_id": project_id},
        )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        DELETE FROM resourcelimit
        WHERE entity_type = 'project'
          AND resource = 'storage_size'
          AND max_total = 0
          AND max_per_branch = 0
    """)
