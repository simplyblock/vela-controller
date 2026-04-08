"""Celery tasks that periodically checks WAL PVC utilization and perform
cleanup of WAL.

The task fetches all PITR-enabled branches and dispatches an independent
`wal_cleanup_branch` sub-task for each one.
"""

import contextlib
import logging
import time
from functools import lru_cache
from typing import cast

import asyncpg
from asgiref.sync import async_to_sync
from asyncpg import exceptions as asyncpg_exceptions
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select
from ulid import ULID

from ..exceptions import (
    VelaDeploymentError,
    VelaKubernetesError,
    VelaSimplyblockAPIError,
    VelaWALPVCError,
)
from ..models.branch import Branch
from ..worker import app
from . import (
    AUTOSCALER_PVC_SUFFIX,
    branch_db_domain,
    get_autoscaler_vm_identity,
    resolve_autoscaler_wal_volume_identifiers,
)
from .kubernetes._util import _ensure_kubeconfig
from .kubernetes.neonvm import resolve_autoscaler_vm_pod_name
from .kubernetes.snapshot import create_snapshot_from_pvc
from .simplyblock_api import create_simplyblock_api

logger = logging.getLogger(__name__)

WAL_UTILIZATION_THRESHOLD = 0.90


@lru_cache
def _session_factory() -> async_sessionmaker:
    from ..api.settings import get_settings

    engine = create_async_engine(
        str(get_settings().postgres_url),
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def _get_wal_utilization(branch: Branch) -> float:
    """Return WAL PVC utilization as a ratio (0.0–1.0)."""
    namespace, _ = get_autoscaler_vm_identity(branch.id)
    try:
        volume_uuid, _ = await resolve_autoscaler_wal_volume_identifiers(namespace)
    except (VelaDeploymentError, ApiException) as exc:
        raise VelaWALPVCError(f"Failed to resolve WAL volume identifiers for branch {branch.id}") from exc

    try:
        async with create_simplyblock_api() as sb_api:
            iostats = await sb_api.volume_iostats(volume=volume_uuid)
            volume = await sb_api.get_volume(volume=volume_uuid)
    except VelaSimplyblockAPIError as exc:
        raise VelaWALPVCError(f"Failed to fetch WAL volume stats for branch {branch.id}") from exc

    size_used: int = iostats.get("size_used", 0)
    size_total: int = volume.size
    if size_total == 0:
        raise VelaWALPVCError(f"WAL volume for branch {branch.id} reported size 0")
    return size_used / size_total


async def _cleanup_wal(branch: Branch) -> None:
    """
    1. Get the SAFE WAL file name
    2. Take the snapshot of DATA PVC
    3. Trigger cleanup using `pg_archivecleanup` binary by perform kubectl exec
    """
    db_host = branch_db_domain(branch.id)
    connection = None
    try:
        connection = await asyncpg.connect(
            user="supabase_admin",
            password=branch.database_password,
            database=branch.database,
            host=db_host,
            port=5432,
            server_settings={"application_name": "vela-wal-compact"},
            command_timeout=10,
        )
        safe_wal = await connection.fetchval("SELECT pg_walfile_name(redo_lsn) FROM pg_control_checkpoint();")
    except (asyncpg_exceptions.PostgresError, OSError):
        logger.exception("Failed to connect to database for branch %s to determine safe WAL", branch.id)
        return
    finally:
        with contextlib.suppress(Exception):
            if connection is not None:
                await connection.close()

    if not safe_wal:
        logger.warning("Safe WAL query returned null for branch %s", branch.id)
        return

    namespace, vm_name = get_autoscaler_vm_identity(branch.id)
    pvc_name = f"{vm_name}{AUTOSCALER_PVC_SUFFIX}"
    snapshot_name = f"{str(branch.id).lower()}-compact-{int(time.time())}"[:63]

    try:
        await create_snapshot_from_pvc(
            namespace=namespace,
            name=snapshot_name,
            snapshot_class="simplyblock-csi-snapshotclass",
            pvc_name=pvc_name,
        )
        logger.info("Created WAL compaction snapshot %s for branch %s", snapshot_name, branch.id)
    except ApiException:
        logger.exception("Failed to create snapshot for branch %s before WAL cleanup", branch.id)
        return

    try:
        from kubernetes_asyncio.stream import WsApiClient

        pod_name = await resolve_autoscaler_vm_pod_name(namespace, vm_name)
        cmd = ["ssh", "guest-vm", "pg_archivecleanup", "/var/lib/postgresql/wal/pg_wal", safe_wal]
        await _ensure_kubeconfig()
        async with WsApiClient() as ws_api:
            core_v1 = k8s_client.CoreV1Api(api_client=ws_api)
            resp = await core_v1.connect_get_namespaced_pod_exec(
                pod_name,
                namespace,
                command=cast("str", cmd),
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        logger.info(
            "pg_archivecleanup for branch %s up to %s completed. Output: %s",
            branch.id,
            safe_wal,
            resp,
        )
    except (ApiException, RuntimeError, VelaKubernetesError):
        logger.warning("Failed to run pg_archivecleanup for branch %s", branch.id, exc_info=True)


async def _wal_cleanup_branch(branch_id: ULID) -> None:
    async with _session_factory()() as db:
        branch = await db.get(Branch, branch_id)

    if branch is None:
        logger.warning("Branch %s not found, skipping WAL cleanup", branch_id)
        return

    try:
        utilization = await _get_wal_utilization(branch)
    except VelaWALPVCError as exc:
        logger.error("%s", exc)
        return

    logger.info("WAL PVC utilization for branch %s: %.1f%%", branch.id, utilization * 100)

    if utilization >= WAL_UTILIZATION_THRESHOLD:
        logger.warning(
            "WAL PVC for branch %s at %.1f%% — triggering compaction",
            branch.id,
            utilization * 100,
        )
        await _cleanup_wal(branch)


async def _wal_cleanup() -> None:
    async with _session_factory()() as db:
        result = await db.execute(select(Branch).where(Branch.pitr_enabled == True))  # noqa: E712
        branch_ids = [b.id for b in result.scalars().all()]

    logger.info("WAL cleanup: dispatching tasks for %d PITR-enabled branches", len(branch_ids))
    for branch_id in branch_ids:
        # sub-tasks run in parallel across the worker pool.
        wal_cleanup_branch.delay(str(branch_id))


@app.task(name="simplyblock.vela.deployment.wal_cleanup.wal_cleanup_branch")
def wal_cleanup_branch(branch_id: str) -> None:
    """Check WAL PVC utilization for a single branch and compact if >= 90%."""
    async_to_sync(_wal_cleanup_branch)(ULID.from_str(branch_id))


@app.task(name="simplyblock.vela.deployment.wal_cleanup.wal_cleanup")
def wal_cleanup() -> None:
    """Periodic beat task: dispatch a wal_cleanup_branch task for every PITR-enabled branch."""
    async_to_sync(_wal_cleanup)()
