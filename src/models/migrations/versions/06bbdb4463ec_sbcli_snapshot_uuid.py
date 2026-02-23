"""sbcli_snapshot_uuid

Revision ID: 06bbdb4463ec
Revises: d4d5cfef8982
Create Date: 2026-02-16 08:02:15.105663

"""
import asyncio
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql
from simplyblock.vela.deployment import load_simplyblock_credentials
from simplyblock.vela.deployment.simplyblock_api import SimplyblockPoolApi


# revision identifiers, used by Alembic.
revision: str = '06bbdb4463ec'
down_revision: Union[str, Sequence[str], None] = 'd4d5cfef8982'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


async def _list_snapshot_ids_by_name() -> dict[str, str]:
    endpoint, cluster_id, cluster_secret, pool_name = await load_simplyblock_credentials()
    async with SimplyblockPoolApi(endpoint, cluster_id, cluster_secret, pool_name) as sb_api:
        snapshots = await sb_api.list_snapshots()

    snapshot_ids_by_name: dict[str, str] = {}
    duplicate_names: set[str] = set()
    for snapshot in snapshots:
        if snapshot.name in snapshot_ids_by_name:
            duplicate_names.add(snapshot.name)
            continue
        snapshot_ids_by_name[snapshot.name] = str(snapshot.id)

    if duplicate_names:
        duplicate_names_list = ", ".join(sorted(duplicate_names))
        raise RuntimeError(f"Duplicate Simplyblock snapshot names found: {duplicate_names_list}")

    return snapshot_ids_by_name


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('backupentry', sa.Column('snapshot_uuid', sa.String(length=64), nullable=True))

    bind = op.get_bind()
    backup_rows = bind.execute(
        sa.text(
            """
            SELECT id::text AS id, snapshot_name
            FROM backupentry
            WHERE snapshot_uuid IS NULL
            """
        )
    ).mappings().all()

    if not backup_rows:
        op.alter_column('backupentry', 'snapshot_uuid', existing_type=sa.String(length=64), nullable=False)
        return

    rows_missing_snapshot_name = [row["id"] for row in backup_rows if row["snapshot_name"] is None]
    if rows_missing_snapshot_name:
        missing_name_ids = ", ".join(sorted(rows_missing_snapshot_name))
        raise RuntimeError(f"Cannot backfill snapshot_uuid: missing snapshot_name for backupentry IDs {missing_name_ids}")

    snapshot_ids_by_name = asyncio.run(_list_snapshot_ids_by_name())

    missing_snapshot_names = sorted(
        {row["snapshot_name"] for row in backup_rows if row["snapshot_name"] not in snapshot_ids_by_name}
    )
    if missing_snapshot_names:
        missing_names = ", ".join(missing_snapshot_names)
        raise RuntimeError(f"Cannot backfill snapshot_uuid: snapshots not found in Simplyblock API: {missing_names}")

    bind.execute(
        sa.text(
            """
            UPDATE backupentry
            SET snapshot_uuid = :snapshot_uuid
            WHERE id = CAST(:id AS uuid)
            """
        ),
        [{"id": row["id"], "snapshot_uuid": snapshot_ids_by_name[row["snapshot_name"]]} for row in backup_rows],
    )

    remaining_null_count = bind.execute(
        sa.text(
            """
            SELECT count(*)
            FROM backupentry
            WHERE snapshot_uuid IS NULL
            """
        )
    ).scalar_one()
    if remaining_null_count > 0:
        raise RuntimeError(f"Backfill incomplete: {remaining_null_count} backupentry rows still have NULL snapshot_uuid")

    op.alter_column('backupentry', 'snapshot_uuid', existing_type=sa.String(length=64), nullable=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('backupentry', 'snapshot_uuid', existing_type=sa.String(length=64), nullable=True)
    op.drop_column('backupentry', 'snapshot_uuid')
