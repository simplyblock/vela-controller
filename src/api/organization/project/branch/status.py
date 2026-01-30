import logging
from datetime import UTC, datetime, timedelta
from typing import cast

from pydantic import ValidationError

from ....._util import Identifier
from .....deployment.monitors.health import vm_monitor
from .....models.branch import Branch, BranchResizeStatus, BranchResizeStatusEntry, BranchServiceStatus, BranchStatus
from ....db import AsyncSessionLocal

logger = logging.getLogger(__name__)


_CREATING_STATUS_ERROR_GRACE_PERIOD = timedelta(minutes=5)
_STARTING_STATUS_ERROR_GRACE_PERIOD = timedelta(minutes=5)
_TRANSITIONAL_BRANCH_STATUSES: set[BranchServiceStatus] = {
    BranchServiceStatus.CREATING,
    BranchServiceStatus.STARTING,
    BranchServiceStatus.STOPPING,
    BranchServiceStatus.RESTARTING,
    BranchServiceStatus.PAUSING,
    BranchServiceStatus.RESUMING,
    BranchServiceStatus.UPDATING,
    BranchServiceStatus.DELETING,
    BranchServiceStatus.RESIZING,
}
_PROTECTED_BRANCH_STATUSES: set[BranchServiceStatus] = {BranchServiceStatus.PAUSED}
_ACTIVE_RESIZE_STATUSES: set[BranchResizeStatus] = {
    "PENDING",
    "RESIZING",
    "FILESYSTEM_RESIZE_PENDING",
}


def parse_branch_status(value: BranchServiceStatus | str | None) -> BranchServiceStatus:
    if isinstance(value, BranchServiceStatus):
        return value
    if value:
        # Normalize to the canonical representation expected by the enum ("STARTING", "STOPPED", etc.).
        normalized_value = str(value).upper()
        member = BranchServiceStatus._value2member_map_.get(normalized_value)
        if member is not None:
            return cast("BranchServiceStatus", member)
        logger.warning("Encountered unknown branch status %s; defaulting to UNKNOWN", value)
    return BranchServiceStatus.UNKNOWN


async def persist_branch_status(branch_id: Identifier, status: BranchServiceStatus) -> None:
    async with AsyncSessionLocal() as session:
        branch = await session.get(Branch, branch_id)
        if branch is None:
            logger.warning("Branch %s missing while updating status to %s", branch_id, status)
            return
        if parse_branch_status(branch.status) == status:
            return
        branch.set_status(status)
        await session.commit()


def _should_update_branch_status(
    current: BranchServiceStatus,
    derived: BranchServiceStatus,
    resize_status: BranchResizeStatus,
) -> bool:
    if current == derived:
        return False
    if current == BranchServiceStatus.RESIZING and resize_status in _ACTIVE_RESIZE_STATUSES:
        # Keep the explicit RESIZING while a resize is still in progress
        # unless we detect a hard failure.
        return derived == BranchServiceStatus.ERROR
    if current == BranchServiceStatus.STARTING and derived == BranchServiceStatus.STOPPED:
        logger.debug("Ignoring STARTING -> STOPPED transition detected by branch status monitor")
        return False
    if current in _PROTECTED_BRANCH_STATUSES and derived not in {
        BranchServiceStatus.ACTIVE_HEALTHY,
        BranchServiceStatus.ERROR,
    }:
        return False
    if (
        derived == BranchServiceStatus.STOPPED
        and current in _TRANSITIONAL_BRANCH_STATUSES
        and current != BranchServiceStatus.STOPPING
    ):
        return False
    if derived in {
        BranchServiceStatus.ACTIVE_HEALTHY,
        BranchServiceStatus.ACTIVE_UNHEALTHY,
        BranchServiceStatus.STOPPED,
        BranchServiceStatus.ERROR,
    }:
        return True
    if derived == BranchServiceStatus.UNKNOWN:
        return current not in _TRANSITIONAL_BRANCH_STATUSES and current not in _PROTECTED_BRANCH_STATUSES
    return True


def _adjust_derived_status_for_stuck_creation(
    branch: Branch, current: BranchServiceStatus, derived: BranchServiceStatus
) -> BranchServiceStatus:
    if derived != BranchServiceStatus.STOPPED:
        return derived

    status_timestamp = branch.status_updated_at or branch.created_datetime
    elapsed = datetime.now(UTC) - status_timestamp

    if current == BranchServiceStatus.CREATING and elapsed >= _CREATING_STATUS_ERROR_GRACE_PERIOD:
        logger.warning(
            "Branch %s still CREATING after %s with STOPPED services; marking ERROR",
            branch.id,
            elapsed,
        )
        return BranchServiceStatus.ERROR

    if current == BranchServiceStatus.STARTING and elapsed >= _STARTING_STATUS_ERROR_GRACE_PERIOD:
        logger.warning(
            "Branch %s still STARTING after %s with STOPPED services; marking ERROR",
            branch.id,
            elapsed,
        )
        return BranchServiceStatus.ERROR

    return derived


async def refresh_branch_status(branch_id: Identifier) -> BranchServiceStatus:
    """
    Probe branch services, derive an overall lifecycle state, and persist it when appropriate.
    """
    async with AsyncSessionLocal() as session:
        branch = await session.get(Branch, branch_id)
        if branch is None:
            logger.warning("Branch %s not found while refreshing status", branch_id)
            return BranchServiceStatus.UNKNOWN

        current_status = parse_branch_status(branch.status)
        try:
            service_status = await collect_branch_service_health(branch_id)
            derived_status = derive_branch_status_from_services(
                service_status,
                storage_enabled=branch.enable_file_storage,
            )
        except Exception:
            logger.exception("Failed to refresh service status for branch %s", branch.id)
            derived_status = BranchServiceStatus.UNKNOWN

        derived_status = _adjust_derived_status_for_stuck_creation(
            branch,
            current_status,
            derived_status,
        )

        if _should_update_branch_status(
            current_status,
            derived_status,
            resize_status=branch.resize_status,
        ):
            branch.set_status(derived_status)
            await session.commit()
            return derived_status

        await session.rollback()
        return current_status


def normalize_resize_statuses(branch: Branch) -> dict[str, BranchResizeStatusEntry]:
    statuses = branch.resize_statuses or {}
    if not statuses:
        return {}

    normalized: dict[str, BranchResizeStatusEntry] = {}
    for service, entry in statuses.items():
        if isinstance(entry, BranchResizeStatusEntry):
            normalized[service] = entry
            continue
        try:
            normalized[service] = BranchResizeStatusEntry.model_validate(entry)
        except ValidationError:
            logger.warning(
                "Skipping invalid resize status entry for branch %s service %s",
                branch.id,
                service,
            )
    return normalized


def derive_branch_status_from_services(
    service_status: BranchStatus,
    *,
    storage_enabled: bool,
) -> BranchServiceStatus:
    statuses: list[BranchServiceStatus] = [
        service_status.database,
        service_status.meta,
        service_status.rest,
    ]
    if storage_enabled:
        statuses.append(service_status.storage)

    if all(status == BranchServiceStatus.ACTIVE_HEALTHY for status in statuses):
        return BranchServiceStatus.ACTIVE_HEALTHY
    if any(status == BranchServiceStatus.ERROR for status in statuses):
        return BranchServiceStatus.ERROR
    if all(status == BranchServiceStatus.STOPPED for status in statuses):
        return BranchServiceStatus.STOPPED
    if any(status == BranchServiceStatus.UNKNOWN for status in statuses):
        return BranchServiceStatus.UNKNOWN
    return BranchServiceStatus.ACTIVE_UNHEALTHY


async def collect_branch_service_health(id_: Identifier) -> BranchStatus:
    status = vm_monitor.status(id_)
    if status is None or status.services is None:
        return BranchStatus(
            database=BranchServiceStatus.UNKNOWN,
            storage=BranchServiceStatus.UNKNOWN,
            meta=BranchServiceStatus.UNKNOWN,
            rest=BranchServiceStatus.UNKNOWN,
        )

    services = status.services
    return BranchStatus(
        database=BranchServiceStatus.ACTIVE_HEALTHY if services.get("postgres", False) else BranchServiceStatus.STOPPED,
        storage=BranchServiceStatus.ACTIVE_HEALTHY
        if services.get("storageapi", False)
        else BranchServiceStatus.STOPPED,
        meta=BranchServiceStatus.ACTIVE_HEALTHY if services.get("meta", False) else BranchServiceStatus.STOPPED,
        rest=BranchServiceStatus.ACTIVE_HEALTHY if services.get("rest", False) else BranchServiceStatus.STOPPED,
    )
