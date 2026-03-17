"""Celery task and dispatch helper for branch resize operations.

The task lives in the deployment layer and has no DB access.  The API layer
manages branch state; this module only orchestrates infrastructure changes and
returns the resulting sizes for the caller to persist.
"""

import logging

from asgiref.sync import async_to_sync
from ulid import ULID

from ..worker import app
from . import (
    AUTOSCALER_PVC_SUFFIX,
    STORAGE_PVC_SUFFIX,
    get_autoscaler_vm_identity,
    kube_service,
    update_branch_volume_iops,
)

logger = logging.getLogger(__name__)


async def _resize_cpu_memory(deployment_id: ULID, milli_vcpu: int | None, memory_bytes: int | None) -> None:
    namespace, autoscaler_vm_name = get_autoscaler_vm_identity(deployment_id)
    await kube_service.resize_autoscaler_vm(
        namespace, autoscaler_vm_name, cpu_milli=milli_vcpu, memory_bytes=memory_bytes
    )


@app.task(name="simplyblock.vela.deployment.resize.cpu_memory")
def resize_cpu_memory(deployment_id: str, milli_vcpu: int | None, memory_bytes: int | None) -> None:
    """Resize CPU and/or memory for a branch deployment."""
    async_to_sync(_resize_cpu_memory)(ULID.from_str(deployment_id), milli_vcpu, memory_bytes)


async def _resize_iops(deployment_id: ULID, iops: int) -> None:
    await update_branch_volume_iops(deployment_id, iops)


@app.task(name="simplyblock.vela.deployment.resize.iops")
def resize_iops(deployment_id: str, iops: int) -> None:
    """Resize IOPS for a branch deployment."""
    async_to_sync(_resize_iops)(ULID.from_str(deployment_id), iops)


async def _resize_pvc(deployment_id: ULID, pvc_suffix: str, target_size: int) -> None:
    namespace, autoscaler_vm_name = get_autoscaler_vm_identity(deployment_id)
    pvc_name = f"{autoscaler_vm_name}{pvc_suffix}"
    await kube_service.resize_pvc_storage(namespace, pvc_name, str(target_size), wait=True)


@app.task(name="simplyblock.vela.deployment.resize.database_pvc")
def resize_database_pvc(deployment_id: str, database_size: int) -> None:
    """Resize the database PVC for a branch deployment."""
    async_to_sync(_resize_pvc)(ULID.from_str(deployment_id), AUTOSCALER_PVC_SUFFIX, database_size)


@app.task(name="simplyblock.vela.deployment.resize.storage_pvc")
def resize_storage_pvc(deployment_id: str, storage_size: int) -> None:
    """Resize the storage PVC for a branch deployment."""
    async_to_sync(_resize_pvc)(ULID.from_str(deployment_id), STORAGE_PVC_SUFFIX, storage_size)
