"""establish-access-rights

Revision ID: f76768ab95d8
Revises: 6726a34eb0ec
Create Date: 2026-03-26 08:20:53.641921

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'f76768ab95d8'
down_revision: Union[str, Sequence[str], None] = '6726a34eb0ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACCESS_RIGHTS = [
    ("org:owner:admin", "organization"),
    ("org:settings:read", "organization"),
    ("org:settings:admin", "organization"),
    ("org:auth:read", "organization"),
    ("org:auth:admin", "organization"),
    ("org:backup:read", "organization"),
    ("org:backup:update", "organization"),
    ("org:backup:create", "organization"),
    ("org:backup:delete", "organization"),
    ("org:metering:read", "organization"),
    ("org:role:read", "organization"),
    ("org:role:admin", "organization"),
    ("org:user:read", "organization"),
    ("org:user:admin", "organization"),
    ("org:role-assign:read", "organization"),
    ("org:role-assign:admin", "organization"),
    ("org:projects:read", "organization"),
    ("org:projects:write", "organization"),
    ("org:projects:create", "organization"),
    ("org:projects:stop", "organization"),
    ("org:projects:pause", "organization"),
    ("org:projects:delete", "organization"),
    ("org:projects:apikeys", "organization"),
    ("org:limits:read", "organization"),
    ("org:limits:admin", "organization"),
    ("env:db:admin", "environment"),
    ("env:projects:read", "environment"),
    ("env:projects:admin", "environment"),
    ("env:backup:read", "environment"),
    ("env:backup:admin", "environment"),
    ("env:projects:write", "environment"),
    ("env:projects:create", "environment"),
    ("env:role-assign:read", "environment"),
    ("env:role-assign:admin", "environment"),
    ("env:projects:stop", "environment"),
    ("env:projects:pause", "environment"),
    ("env:projects:delete", "environment"),
    ("env:projects:getkeys", "environment"),
    ("project:settings:read", "project"),
    ("project:settings:write", "project"),
    ("project:role-assign:read", "project"),
    ("project:role-assign:admin", "project"),
    ("project:branches:create", "project"),
    ("project:branches:delete", "project"),
    ("project:branches:stop", "project"),
    ("branch:settings:read", "branch"),
    ("branch:settings:admin", "branch"),
    ("branch:role-assign:read", "branch"),
    ("branch:role-assign:admin", "branch"),
    ("branch:auth:read", "branch"),
    ("branch:auth:admin", "branch"),
    ("branch:api:getkeys", "branch"),
    ("branch:replicate:read", "branch"),
    ("branch:replicate:admin", "branch"),
    ("branch:import:read", "branch"),
    ("branch:import:admin", "branch"),
    ("branch:logging:read", "branch"),
    ("branch:monitoring:read", "branch"),
    ("branch:db:admin", "branch"),
    ("branch:rls:read", "branch"),
    ("branch:rls:admin", "branch"),
    ("branch:edge:read", "branch"),
    ("branch:edge:admin", "branch"),
    ("branch:rt:read", "branch"),
    ("branch:rt:admin", "branch"),
]


def upgrade() -> None:
    """Upgrade schema."""
    conn = op.get_bind()
    for entry, role_type in _ACCESS_RIGHTS:
        conn.execute(
            sa.text(
                "INSERT INTO accessright (id, entry, role_type) "
                "SELECT gen_random_uuid(), :entry, :role_type "
                "WHERE NOT EXISTS (SELECT 1 FROM accessright WHERE entry = :entry_check)"
            ),
            {"entry": entry, "role_type": role_type, "entry_check": entry},
        )


def downgrade() -> None:
    """Downgrade schema."""
    conn = op.get_bind()
    conn.execute(
        sa.text("DELETE FROM accessright WHERE entry = ANY(:entries)").bindparams(
            sa.bindparam("entries", value=[e for e, _ in _ACCESS_RIGHTS], type_=sa.ARRAY(sa.String))
        )
    )
