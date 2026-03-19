"""Add and backfill branch.db_port

Revision ID: 2b4e8f1a6c03
Revises: f4f677e4e9b9
Create Date: 2026-03-19 00:00:00.000000

"""
import logging
import os
from typing import Sequence, Union
from uuid import UUID

from alembic import op
import sqlalchemy as sa
from kubernetes import client as kubernetes_client
from kubernetes import config as kubernetes_config
from kubernetes.config.config_exception import ConfigException
from ulid import ULID

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = '2b4e8f1a6c03'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_CHART_NAME = "vela"


def _ensure_kube_client_config() -> None:
    try:
        kubernetes_config.load_incluster_config()
    except ConfigException:
        try:
            kubernetes_config.load_kube_config()
        except ConfigException as exc:
            raise RuntimeError("Kubernetes client not configured. Mount kubeconfig or run in-cluster.") from exc


def _uuid_to_ulid_str(uuid_str: str) -> str:
    """Convert a UUID string (as stored in the DB) back to a ULID string."""
    return str(ULID.from_bytes(UUID(uuid_str).bytes))


def _deployment_namespace(branch_id: str) -> str:
    prefix = os.environ.get("VELA_DEPLOYMENT_NAMESPACE_PREFIX", "vela")
    ulid_str = _uuid_to_ulid_str(branch_id).lower()
    return f"{prefix}-{ulid_str}" if prefix else ulid_str


def _release_fullname() -> str:
    release = os.environ.get("VELA_DEPLOYMENT_RELEASE_NAME", "vela")
    return release if _CHART_NAME in release else f"{release}-{_CHART_NAME}"


def _branch_service_name(component: str) -> str:
    return f"{_release_fullname()}-{component}"


def _get_all_node_ports() -> dict[str, int]:
    """Fetch node ports for all pgbouncer/db services in a single API call.

    Returns a mapping of namespace -> nodePort, preferring the pgbouncer service
    over the db service when both exist in the same namespace.
    """
    core_v1 = kubernetes_client.CoreV1Api()
    target_names = {_branch_service_name("pgbouncer"), _branch_service_name("db")}

    svc_list = core_v1.list_service_for_all_namespaces()

    ports_by_namespace: dict[str, int] = {}
    for svc in svc_list.items:
        if svc.metadata.name not in target_names:
            continue
        ns = svc.metadata.namespace
        if ns in ports_by_namespace:
            continue  # already resolved for this namespace
        ports = svc.spec.ports
        if ports and ports[0].node_port:
            ports_by_namespace[ns] = ports[0].node_port

    return ports_by_namespace


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'branch',
        sa.Column('db_port', sa.Integer(), nullable=True),
    )

    bind = op.get_bind()
    branch_rows = bind.execute(
        sa.text("SELECT id::text AS id FROM branch")
    ).mappings().all()

    if not branch_rows:
        return

    _ensure_kube_client_config()
    ports_by_namespace = _get_all_node_ports()

    updates = []
    for row in branch_rows:
        namespace = _deployment_namespace(row["id"])
        port = ports_by_namespace.get(namespace)
        if port is None:
            logger.warning("Could not resolve db_port for branch %s — skipping", row["id"])
            continue
        updates.append({"id": row["id"], "db_port": port})

    if updates:
        bind.execute(
            sa.text(
                """
                UPDATE branch
                SET db_port = :db_port
                WHERE id = CAST(:id AS uuid)
                """
            ),
            updates,
        )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('branch', 'db_port')
