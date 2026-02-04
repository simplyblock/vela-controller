"""Fix role user link

Revision ID: 6bc8fa2681d1
Revises: f4f677e4e9b9
Create Date: 2026-02-04 00:48:42.050409

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql
from ulid import ULID


# revision identifiers, used by Alembic.
revision: str = '6bc8fa2681d1'
down_revision: Union[str, Sequence[str], None] = 'f4f677e4e9b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('roleuserlink', sa.Column('id', sa.UUID(as_uuid=True), nullable=True))

    connection = op.get_bind()
    result = connection.execute(sa.text("SELECT * FROM roleuserlink WHERE id IS NULL"))
    rows = result.fetchall()

    for row in rows:
        connection.execute(
            sa.text("""
                UPDATE roleuserlink 
                SET id = :new_id
                WHERE organization_id = :org_id 
                  AND role_id = :role_id 
                  AND user_id = :user_id
                  AND (env_type = :env_type OR (env_type IS NULL AND :env_type IS NULL))
                  AND (project_id = :project_id OR (project_id IS NULL AND :project_id IS NULL))
                  AND (branch_id = :branch_id OR (branch_id IS NULL AND :branch_id IS NULL))
                  AND id IS NULL
            """),
            {
                'new_id': ULID().to_uuid(),
                'org_id': row.organization_id,
                'role_id': row.role_id,
                'user_id': row.user_id,
                'env_type': row.env_type,
                'project_id': row.project_id,
                'branch_id': row.branch_id,
            }
        )

    op.alter_column('roleuserlink', 'id', nullable=False)
    op.drop_constraint('roleuserlink_pkey', 'roleuserlink', type_='primary')
    op.create_primary_key('roleuserlink_pkey', 'roleuserlink', ['id'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('roleuserlink_pkey', 'roleuserlink', type_='primary')
    op.drop_column('roleuserlink', 'id')
    op.create_primary_key(
        'roleuserlink_pkey',
        'roleuserlink',
        ['organization_id', 'role_id', 'user_id', 'env_type', 'project_id', 'branch_id'],
    )
    op.drop_column('roleuserlink', 'id')
