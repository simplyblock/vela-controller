"""Add and backfill branch.db_port

Revision ID: 2b4e8f1a6c03
Revises: f4f677e4e9b9
Create Date: 2026-03-19 00:00:00.000000

"""
import logging
import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from kubernetes import client as kubernetes_client
from kubernetes import config as kubernetes_config
from kubernetes.config.config_exception import ConfigException

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = '2b4e8f1a6c03'
down_revision: Union[str, Sequence[str], None] = '9bebcc605033'
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


def _deployment_namespace(branch_id: str) -> str:
    prefix = os.environ.get("VELA_DEPLOYMENT_NAMESPACE_PREFIX", "vela")
    return f"{prefix}-{branch_id.lower()}" if prefix else branch_id.lower()


def _release_fullname() -> str:
    release = os.environ.get("VELA_DEPLOYMENT_RELEASE_NAME", "vela")
    return release if _CHART_NAME in release else f"{release}-{_CHART_NAME}"


def _branch_service_name(component: str) -> str:
    return f"{_release_fullname()}-{component}"


def _get_node_port(branch_id: str) -> int | None:
    core_v1 = kubernetes_client.CoreV1Api()
    namespace = _deployment_namespace(branch_id)
    for service_name in (_branch_service_name("pgbouncer"), _branch_service_name("db")):
        try:
            svc = core_v1.read_namespaced_service(name=service_name, namespace=namespace)
            ports = svc.spec.ports
            if ports and ports[0].node_port:
                return ports[0].node_port
        except kubernetes_client.exceptions.ApiException as exc:
            if exc.status == 404:
                continue
            raise
    return None


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

    updates = []
    for row in branch_rows:
        port = _get_node_port(row["id"])
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
