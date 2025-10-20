import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.deployment import get_db_vmi_identity

from ...._util import quantity_to_bytes
from ....api.db import engine
from ....api.models.branch import Branch, aggregate_resize_statuses
from ....api.organization.project.branch import _sync_branch_cpu_resources
from ....deployment import kube_service
from ....exceptions import VelaKubernetesError

logger = logging.getLogger(__name__)

MEMORY_SERVICE_KEY = "database_memory_resize"
MEMORY_POLL_SECONDS = 15


async def refresh_memory_status(session: AsyncSession, branch: Branch) -> None:
    """Confirm memory resize progress by inspecting the VM's pod allocation."""
    branch_id = branch.id
    raw_statuses = dict(branch.resize_statuses or {})
    raw_entry = raw_statuses.get(MEMORY_SERVICE_KEY)
    if raw_entry is None:
        return

    entry_payload = _load_resize_entry(raw_entry)
    entry_payload.pop("requested_value", None)
    current_status = entry_payload.get("status", "PENDING")

    namespace, vmi_name = get_db_vmi_identity(branch.id)
    pod_memory_bytes: int | None = None

    try:
        _pod_name, pod_memory_quantity = await kube_service.get_vm_pod_name(namespace, vmi_name)
    except VelaKubernetesError as exc:
        logger.debug("Waiting for VM pod during memory resize for branch %s: %s", branch.id, exc)
        pod_memory_quantity = None
    else:
        pod_memory_bytes = quantity_to_bytes(pod_memory_quantity)

    target_memory = await kube_service.get_vm_memory_bytes(namespace, vmi_name)
    if target_memory is None:
        # logger.error(
        #     "Unable to determine target VM memory for branch %s; deferring status update",
        #     branch.id,
        # )
        return

    pod_satisfies_request = (
        pod_memory_bytes is not None and target_memory is not None and pod_memory_bytes >= target_memory
    )
    new_status = "COMPLETED" if pod_satisfies_request else "RESIZING"

    branch_memory_needs_update = new_status == "COMPLETED" and branch.memory != target_memory
    state_changed = new_status != current_status
    if not state_changed and not branch_memory_needs_update:
        return

    entry_payload["status"] = new_status
    entry_payload["timestamp"] = _timestamp_now()

    raw_statuses[MEMORY_SERVICE_KEY] = entry_payload
    branch.resize_statuses = raw_statuses
    branch.resize_status = aggregate_resize_statuses(raw_statuses)

    logger.info("1. Memory resize for branch %s completed to %d bytes", branch_id, target_memory)

    if new_status == "COMPLETED":
        branch.memory = target_memory
        logger.info("2. Memory resize for branch %s completed to %d bytes", branch_id, target_memory)
        try:
            await _sync_branch_cpu_resources(
                branch_id,
                desired_milli_vcpu=branch.milli_vcpu,
            )
        except VelaKubernetesError:
            logger.exception(
                "Failed to sync CPU resources for branch %s after memory resize completion",
                branch_id,
            )

    await session.commit()
    logger.info("Updated memory resize status for branch %s to %s", branch_id, new_status)


def _timestamp_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_resize_entry(entry: Any) -> dict[str, Any]:
    if isinstance(entry, dict):
        return dict(entry)
    if hasattr(entry, "model_dump"):
        data = entry.model_dump()
        extra = getattr(entry, "model_extra", None)
        if isinstance(extra, dict):
            data.update(extra)
        return data
    fallback: dict[str, Any] = {}
    for key in ("status", "timestamp"):
        if hasattr(entry, key):
            fallback[key] = getattr(entry, key)
    return fallback


async def reconcile_memory_resizes() -> None:
    """Walk all branches with resize activity and refresh their memory state."""

    # get all the branch ids with memory resize activity
    resize_statuses_column = Branch.resize_statuses
    async with AsyncSession(engine) as session:
        result = await session.exec(
            select(Branch).where(resize_statuses_column.has_key(MEMORY_SERVICE_KEY))  # type: ignore[attr-defined]
        )
        branch_ids = [branch.id for branch in result.all()]

    # for each branch, refresh its memory status by checking if the new pod's memory matches the requested
    for branch_id in branch_ids:
        async with AsyncSession(engine) as branch_session:
            branch = await branch_session.get(Branch, branch_id)
            if branch is None:
                continue
            await refresh_memory_status(branch_session, branch)


async def poll_memory_resizes(stop_event: asyncio.Event) -> None:
    """Periodically reconcile memory resizes until cancelled."""
    while not stop_event.is_set():
        try:
            await reconcile_memory_resizes()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to reconcile memory resize status")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=MEMORY_POLL_SECONDS)
        except TimeoutError:
            continue
