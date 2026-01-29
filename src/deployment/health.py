import asyncio
import logging

from .._util import Identifier
from ..models.branch import BranchServiceStatus, BranchStatus
from .monitors.health import vm_monitor

logger = logging.getLogger(__name__)

SERVICE_PROBE_TIMEOUT_SECONDS = 2

BRANCH_SERVICE_ENDPOINTS: dict[str, tuple[str, int]] = {
    "database": ("db", 5432),
    "pgbouncer": ("pgbouncer", 6432),
    "storage": ("storage", 5000),
    "meta": ("meta", 8080),
    "rest": ("rest", 3000),
    "pgexporter": ("pgexporter", 9187),
}


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


async def probe_service_socket(host: str, port: int, *, label: str) -> BranchServiceStatus:
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host=host, port=port),
            timeout=SERVICE_PROBE_TIMEOUT_SECONDS,
        )
    except (TimeoutError, OSError):
        logger.debug("Service %s unavailable at %s:%s", label, host, port)
        return BranchServiceStatus.STOPPED
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Unexpected error probing service %s", label)
        return BranchServiceStatus.UNKNOWN

    writer.close()
    try:
        await writer.wait_closed()
    except OSError:  # pragma: no cover - best effort socket cleanup
        logger.debug("Failed to close probe socket for %s", label, exc_info=True)
    return BranchServiceStatus.ACTIVE_HEALTHY


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
