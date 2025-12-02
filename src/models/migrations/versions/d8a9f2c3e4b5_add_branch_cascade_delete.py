"""add_branch_cascade_delete

Revision ID: d8a9f2c3e4b5
Revises: c53b3f8367e6
Create Date: 2025-12-02 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'd8a9f2c3e4b5'
down_revision: Union[str, Sequence[str], None] = 'c53b3f8367e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Tables with foreign keys to branch that need ON DELETE CASCADE
BRANCH_FK_TABLES = [
    ('backupentry', 'backupentry_branch_id_fkey'),
    ('backuplog', 'backuplog_branch_id_fkey'),
    ('backupschedule', 'backupschedule_branch_id_fkey'),
    ('branchprovisioning', 'branchprovisioning_branch_id_fkey'),
    ('nextbackup', 'nextbackup_branch_id_fkey'),
    ('pgbouncerconfig', 'pgbouncerconfig_branch_id_fkey'),
    ('provisioninglog', 'provisioninglog_branch_id_fkey'),
    ('resourceusageminute', 'resourceusageminute_branch_id_fkey'),
    ('roleuserlink', 'roleuserlink_branch_id_fkey'),
]


def upgrade() -> None:
    """Add ON DELETE CASCADE to branch foreign keys."""
    for table_name, constraint_name in BRANCH_FK_TABLES:
        op.drop_constraint(constraint_name, table_name, type_='foreignkey')
        op.create_foreign_key(
            constraint_name,
            table_name,
            'branch',
            ['branch_id'],
            ['id'],
            ondelete='CASCADE',
        )


def downgrade() -> None:
    """Remove ON DELETE CASCADE from branch foreign keys."""
    for table_name, constraint_name in BRANCH_FK_TABLES:
        op.drop_constraint(constraint_name, table_name, type_='foreignkey')
        op.create_foreign_key(
            constraint_name,
            table_name,
            'branch',
            ['branch_id'],
            ['id'],
        )
