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


# revision identifiers, used by Alembic.
revision: str = '9bebcc605033'
down_revision: Union[str, Sequence[str], None] = '81dc009bed13'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        INSERT INTO resourcelimit
            (id, entity_type, resource, org_id, env_type, project_id, max_total, max_per_branch)
        SELECT gen_random_uuid(), 'project', 'storage_size', NULL, NULL, p.id, 0, 0
        FROM project p
        WHERE NOT EXISTS (
            SELECT 1 FROM resourcelimit rl
            WHERE rl.project_id = p.id AND rl.resource = 'storage_size'
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        DELETE FROM resourcelimit
        WHERE entity_type = 'project'
          AND resource = 'storage_size'
          AND max_total = 0
          AND max_per_branch = 0
    """)
