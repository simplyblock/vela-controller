"""Celery task and dispatch helper for branch resize operations.

The task lives in the deployment layer and has no DB access.  The API layer
manages branch state; this module only orchestrates infrastructure changes and
returns the resulting sizes for the caller to persist.
"""

import asyncio
import logging
import re
import time
from uuid import UUID

from ulid import ULID

from .._util import quantity_to_bytes, storage_backend_bytes_to_db_bytes
from ..worker import app
from . import AUTOSCALER_PVC_SUFFIX, STORAGE_PVC_SUFFIX, kube_service, update_branch_volume_iops
from .kubernetes._util import core_v1_client

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 5
POLL_TIMEOUT_SECONDS = 30 * 60  # 30 minutes

_FAILURE_PATTERN = re.compile(
    r"\b(resize|resizing|resized)\w*\b.*\b(fail|failure|failed|failing|error|err)\w*\b"
    r"|"
    r"\b(fail|failure|failed|failing|error|err)\w*\b.*\b(resize|resizing|resized)\w*\b",
    flags=re.IGNORECASE,
)


async def _poll_pvc_until_complete(namespace: str, name: str, target_bytes: int) -> int:
    """Poll PVC status every 5s until resize completes or fails. Returns actual capacity."""
    start = time.monotonic()
    async with core_v1_client() as core_v1:
        while True:
            pvc = await core_v1.read_namespaced_persistent_volume_claim(namespace=namespace, name=name)

            # Check if the PVC capacity has reached the target
            capacity_str = (pvc.status.capacity or {}).get("storage")
            if capacity_str:
                actual = quantity_to_bytes(capacity_str)
                if actual and actual >= target_bytes:
                    return storage_backend_bytes_to_db_bytes(actual)

            # Check for resize failure conditions on the PVC
            for condition in pvc.status.conditions or []:
                msg = condition.message or ""
                if _FAILURE_PATTERN.search(msg):
                    raise RuntimeError(f"PVC {namespace}/{name} resize failed: {msg}")

            elapsed = time.monotonic() - start
            if elapsed >= POLL_TIMEOUT_SECONDS:
                raise TimeoutError(f"PVC {namespace}/{name} resize timed out after {POLL_TIMEOUT_SECONDS}s")

            await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _async_perform_resize(
    branch_id: str,
    namespace: str,
    autoscaler_vm_name: str,
    effective_parameters: dict,
) -> dict[str, int | None]:
    database_size: int | None = effective_parameters.get("database_size")
    storage_size: int | None = effective_parameters.get("storage_size")
    milli_vcpu: int | None = effective_parameters.get("milli_vcpu")
    memory_bytes: int | None = effective_parameters.get("memory_bytes")
    iops: int | None = effective_parameters.get("iops")

    result: dict[str, int | None] = {
        "database_size": None,
        "storage_size": None,
        "milli_vcpu": None,
        "memory_bytes": None,
        "iops": None,
    }

    # CPU / memory resize (immediate Kubernetes patch)
    if milli_vcpu is not None or memory_bytes is not None:
        await kube_service.resize_autoscaler_vm(
            namespace,
            autoscaler_vm_name,
            cpu_milli=milli_vcpu,
            memory_bytes=memory_bytes,
        )
        if milli_vcpu is not None:
            result["milli_vcpu"] = milli_vcpu
        if memory_bytes is not None:
            result["memory_bytes"] = memory_bytes

    # IOPS resize (immediate simplyblock API call)
    if iops is not None:
        await update_branch_volume_iops(ULID.from_str(branch_id), iops)
        result["iops"] = iops

    # PVC resize — patch spec then poll until capacity matches
    if database_size is not None:
        pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        await kube_service.resize_pvc_storage(namespace, pvc_name, str(database_size))
        result["database_size"] = await _poll_pvc_until_complete(namespace, pvc_name, database_size)

    if storage_size is not None:
        pvc_name = f"{autoscaler_vm_name}{STORAGE_PVC_SUFFIX}"
        await kube_service.resize_pvc_storage(namespace, pvc_name, str(storage_size))
        result["storage_size"] = await _poll_pvc_until_complete(namespace, pvc_name, storage_size)

    return result


@app.task(name="simplyblock.vela.deployment.resize.perform_resize")
def perform_resize(
    branch_id: str,
    namespace: str,
    autoscaler_vm_name: str,
    effective_parameters: dict,
) -> dict[str, int | None]:
    """Execute resize operations. Returns resulting sizes. Raises on failure."""
    return asyncio.run(
        _async_perform_resize(
            branch_id=branch_id,
            namespace=namespace,
            autoscaler_vm_name=autoscaler_vm_name,
            effective_parameters=effective_parameters,
        )
    )


def dispatch_resize(
    branch_id: str,
    namespace: str,
    autoscaler_vm_name: str,
    effective_parameters: dict,
) -> UUID:
    """Dispatch a resize task and return the Celery task UUID."""
    async_result = perform_resize.apply_async(
        kwargs={
            "branch_id": branch_id,
            "namespace": namespace,
            "autoscaler_vm_name": autoscaler_vm_name,
            "effective_parameters": effective_parameters,
        }
    )
    return UUID(async_result.id)


def get_resize_task_result(task_id: UUID | str):
    """Return the Celery AsyncResult for a resize task."""
    return perform_resize.AsyncResult(str(task_id))
