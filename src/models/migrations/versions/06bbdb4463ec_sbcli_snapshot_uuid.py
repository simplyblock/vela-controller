"""sbcli_snapshot_uuid

Revision ID: 06bbdb4463ec
Revises: d4d5cfef8982
Create Date: 2026-02-16 08:02:15.105663

"""
import re
from typing import Sequence, Union
from uuid import UUID

from kubernetes import client as kubernetes_client
from kubernetes import config as kubernetes_config
from kubernetes.config.config_exception import ConfigException
from alembic import op
import sqlalchemy as sa
import sqlmodel
import sqlmodel.sql


# revision identifiers, used by Alembic.
revision: str = '06bbdb4463ec'
down_revision: Union[str, Sequence[str], None] = 'd4d5cfef8982'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _ensure_kube_client_config() -> None:
    try:
        kubernetes_config.load_incluster_config()
    except ConfigException:
        try:
            kubernetes_config.load_kube_config()
        except ConfigException as exc:
            raise RuntimeError("Kubernetes client not configured. Mount kubeconfig or run in-cluster.") from exc


_SNAPSHOT_HANDLE_RE = re.compile(
    r"(?P<prefix>[^:]+):(?P<id>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)


def _snapshot_id_from_content_name(snapshot_content_name: str) -> str:
    custom = kubernetes_client.CustomObjectsApi()
    content = custom.get_cluster_custom_object(
        group="snapshot.storage.k8s.io",
        version="v1",
        plural="volumesnapshotcontents",
        name=snapshot_content_name,
    )
    snapshot_handle = content.get("status", {}).get("snapshotHandle")
    if not isinstance(snapshot_handle, str):
        raise RuntimeError(f"VolumeSnapshotContent {snapshot_content_name} missing status.snapshotHandle")

    match = _SNAPSHOT_HANDLE_RE.fullmatch(snapshot_handle)
    if match is None:
        raise RuntimeError(
            f"VolumeSnapshotContent {snapshot_content_name} has invalid snapshotHandle: {snapshot_handle}"
        )
    return str(UUID(match.group("id")))


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('backupentry', sa.Column('snapshot_uuid', sa.String(length=64), nullable=True))

    bind = op.get_bind()
    backup_rows = bind.execute(
        sa.text(
            """
            SELECT id::text AS id, snapshot_content_name
            FROM backupentry
            WHERE snapshot_uuid IS NULL
            """
        )
    ).mappings().all()

    if not backup_rows:
        op.alter_column('backupentry', 'snapshot_uuid', existing_type=sa.String(length=64), nullable=False)
        return

    rows_missing_snapshot_content_name = [row["id"] for row in backup_rows if row["snapshot_content_name"] is None]
    if rows_missing_snapshot_content_name:
        missing_content_name_ids = ", ".join(sorted(rows_missing_snapshot_content_name))
        raise RuntimeError(
            "Cannot backfill snapshot_uuid: missing snapshot_content_name for backupentry IDs "
            f"{missing_content_name_ids}"
        )

    _ensure_kube_client_config()
    updates = [
        {"id": row["id"], "snapshot_uuid": _snapshot_id_from_content_name(row["snapshot_content_name"])}
        for row in backup_rows
    ]

    bind.execute(
        sa.text(
            """
            UPDATE backupentry
            SET snapshot_uuid = :snapshot_uuid
            WHERE id = CAST(:id AS uuid)
            """
        ),
        updates,
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
    op.drop_column('backupentry', 'snapshot_uuid')
