import asyncio
import logging

from ..models.branch import BranchServiceStatus, BranchStatus
from . import branch_service_name

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


async def collect_branch_service_health(namespace: str, *, storage_enabled: bool) -> BranchStatus:
    endpoints = {
        label: (branch_service_name(component), port)
        for label, (component, port) in BRANCH_SERVICE_ENDPOINTS.items()
        if storage_enabled or label != "storage"
    }

    probes = {
        label: asyncio.create_task(
            probe_service_socket(
                host=f"{service_name}.{namespace}.svc.cluster.local",
                port=port,
                label=label,
            )
        )
        for label, (service_name, port) in endpoints.items()
    }

    results: dict[str, BranchServiceStatus] = {}
    for label, task in probes.items():
        try:
            results[label] = await task
        except Exception:  # pragma: no cover - unexpected failures
            logger.exception("Service health probe failed for %s", label)
            results[label] = BranchServiceStatus.UNKNOWN

    return BranchStatus(
        database=results["database"],
        storage=results.get(
            "storage",
            BranchServiceStatus.STOPPED if not storage_enabled else BranchServiceStatus.UNKNOWN,
        ),
        meta=results["meta"],
        rest=results["rest"],
    )
