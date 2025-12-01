import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from ....api.db import engine
from ....models.branch import Branch, BranchResizeStatus, aggregate_resize_statuses

logger = logging.getLogger(__name__)

MEMORY_SERVICE_KEY = "database_memory_resize"
MEMORY_POLL_SECONDS = 15
RESIZE_TIMEOUT_SECONDS = 180
RESIZE_TIMEOUT_SERVICES: tuple[str, ...] = (
    "database_cpu_resize",
    "database_memory_resize",
    "database_iops_resize",
    "database_disk_resize",
    "storage_api_disk_resize",
)
_TIMEOUT_STATUSES: set[BranchResizeStatus] = {"PENDING", "RESIZING", "FILESYSTEM_RESIZE_PENDING"}


async def enforce_resize_timeouts() -> None:
    """Mark any pending resize operations as failed after the timeout threshold."""
    cutoff = datetime.now(UTC) - timedelta(seconds=RESIZE_TIMEOUT_SECONDS)
    statuses_column = Branch.resize_statuses
    service_filters = [
        statuses_column.has_key(service)  # type: ignore[attr-defined]
        for service in RESIZE_TIMEOUT_SERVICES
    ]
    if not service_filters:
        return

    query_filter = or_(*service_filters) if len(service_filters) > 1 else service_filters[0]

    async with AsyncSession(engine) as session:
        result = await session.exec(select(Branch).where(query_filter))
        branches = result.all()

        has_changes = False
        for branch in branches:
            statuses = dict(branch.resize_statuses or {})
            branch_updated = False

            for service in RESIZE_TIMEOUT_SERVICES:
                raw_entry = statuses.get(service)
                if raw_entry is None:
                    continue
                entry_payload = _load_resize_entry(raw_entry)
                status_value = entry_payload.get("status")
                if status_value not in _TIMEOUT_STATUSES:
                    continue
                requested_at = _parse_timestamp(entry_payload.get("requested_at") or entry_payload.get("timestamp"))
                if requested_at is None or requested_at > cutoff:
                    continue

                entry_payload["status"] = "FAILED"
                entry_payload["timestamp"] = _timestamp_now()
                statuses[service] = entry_payload
                branch_updated = True
                logger.warning(
                    "Resize operation %s for branch %s timed out after %s seconds; marking as FAILED",
                    service,
                    branch.id,
                    RESIZE_TIMEOUT_SECONDS,
                )

            if branch_updated:
                branch.resize_statuses = statuses
                branch.resize_status = aggregate_resize_statuses(statuses)
                has_changes = True

        if has_changes:
            await session.commit()


def _timestamp_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
