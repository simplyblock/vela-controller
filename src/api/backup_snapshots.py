from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

import httpx

from .._util import Identifier, quantity_to_bytes
from ..deployment import AUTOSCALER_PVC_SUFFIX, get_autoscaler_vm_identity
from ..deployment.kubernetes.snapshot import (
    create_snapshot_from_pvc,
    ensure_snapshot_absent,
    ensure_snapshot_content_absent,
    read_snapshot,
    read_snapshot_content,
    wait_snapshot_ready,
)
from ..deployment.simplyblock_api import create_simplyblock_api
from ..exceptions import VelaKubernetesError, VelaSimplyblockAPIError, VelaSnapshotTimeoutError

if TYPE_CHECKING:
    from ulid import ULID

    from ..models.backups import BackupEntry

logger = logging.getLogger(__name__)

SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
SNAPSHOT_POLL_INTERVAL_SEC = int(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))

_K8S_NAME_MAX_LENGTH = 63
_SNAPSHOT_HANDLE_RE = re.compile(
    r"(?P<prefix>[^:]+):(?P<id>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)


def parse_snapshot_id(handle: str) -> UUID:
    if (match := _SNAPSHOT_HANDLE_RE.fullmatch(handle)) is None:
        raise ValueError("invalid snapshotHandle")
    return UUID(match.group("id"))


@dataclass(frozen=True)
class SnapshotDetails:
    name: str
    namespace: str
    content_name: str | None
    snapshot_uuid: UUID
    size_bytes: int | None


def _sanitize_label(label: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]", "-", label.lower())
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return cleaned or "backup"


def _build_snapshot_name(*, label: str, backup_id: ULID) -> str:
    label_component = _sanitize_label(label)
    backup_component = str(backup_id).lower()

    if not label_component:
        label_component = "backup"

    separator = "-"
    available_for_label = _K8S_NAME_MAX_LENGTH - len(backup_component) - len(separator)
    if available_for_label < 1:
        return backup_component[:_K8S_NAME_MAX_LENGTH]

    label_component = label_component[:available_for_label]
    return f"{label_component}{separator}{backup_component}"


async def create_branch_snapshot(
    branch_id: Identifier,
    *,
    backup_id: ULID,
    snapshot_class: str,
    poll_interval: float,
    label: str,
    time_limit: float,
) -> SnapshotDetails:
    namespace, autoscaler_vm_name = get_autoscaler_vm_identity(branch_id)
    pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"
    snapshot_name = _build_snapshot_name(label=label, backup_id=backup_id)

    logger.info("Creating VolumeSnapshot %s/%s for branch %s", namespace, snapshot_name, branch_id)
    try:
        async with asyncio.timeout(time_limit):
            await create_snapshot_from_pvc(
                namespace=namespace,
                name=snapshot_name,
                snapshot_class=snapshot_class,
                pvc_name=pvc_name,
            )
            snapshot = await wait_snapshot_ready(
                namespace,
                snapshot_name,
                timeout=time_limit,
                poll_interval=poll_interval,
            )
    except TimeoutError as exc:
        logger.exception(
            "Timed out creating VolumeSnapshot %s/%s for branch %s within %s seconds",
            namespace,
            snapshot_name,
            branch_id,
            time_limit,
        )
        raise VelaSnapshotTimeoutError(
            f"Timed out creating VolumeSnapshot {namespace}/{snapshot_name} for branch {branch_id}"
        ) from exc

    status = snapshot.get("status") or {}
    content_name_payload = status.get("boundVolumeSnapshotContentName")
    content_name = content_name_payload if isinstance(content_name_payload, str) else None
    size_bytes = quantity_to_bytes(status.get("restoreSize"))

    try:
        if content_name is None:
            raise ValueError("missing boundVolumeSnapshotContentName")
        handle = (await read_snapshot_content(content_name) or {}).get("status", {}).get("snapshotHandle")
        snapshot_uuid = parse_snapshot_id(handle)
    except (TypeError, ValueError, VelaKubernetesError, httpx.HTTPError) as exc:
        await _cleanup_failed_snapshot_creation(
            namespace=namespace,
            snapshot_name=snapshot_name,
            content_name=content_name,
            time_limit=time_limit,
            poll_interval=poll_interval,
        )
        raise VelaKubernetesError(f"Failed to derive snapshot UUID for snapshot {namespace}/{snapshot_name}") from exc

    logger.info(
        "VolumeSnapshot %s/%s ready (content=%s size_bytes=%s snapshot_uuid=%s)",
        namespace,
        snapshot_name,
        content_name,
        size_bytes,
        snapshot_uuid,
    )

    return SnapshotDetails(
        name=snapshot_name,
        namespace=namespace,
        content_name=content_name,
        snapshot_uuid=snapshot_uuid,
        size_bytes=size_bytes,
    )


async def delete_branch_snapshot(
    *,
    name: str | None,
    namespace: str | None,
    content_name: str | None,
    time_limit: float = SNAPSHOT_TIMEOUT_SEC,
    poll_interval: float = SNAPSHOT_POLL_INTERVAL_SEC,
) -> None:
    if not name or not namespace:
        logger.debug(
            "Skipping deletion for VolumeSnapshot with missing metadata (name=%s namespace=%s)",
            name,
            namespace,
        )
        return

    derived_content_name = content_name
    try:
        async with asyncio.timeout(time_limit):
            snapshot = await read_snapshot(namespace, name)
            if snapshot is not None:
                status = snapshot.get("status") or {}
                derived_content_name = derived_content_name or status.get("boundVolumeSnapshotContentName")
                logger.info("Deleting VolumeSnapshot %s/%s", namespace, name)
                await ensure_snapshot_absent(
                    namespace,
                    name,
                    timeout=time_limit,
                    poll_interval=poll_interval,
                )
            else:
                logger.info("VolumeSnapshot %s/%s already absent", namespace, name)

            if derived_content_name:
                logger.info("Ensuring VolumeSnapshotContent %s is absent", derived_content_name)
                await ensure_snapshot_content_absent(
                    derived_content_name,
                    timeout=time_limit,
                    poll_interval=poll_interval,
                )
    except TimeoutError:
        logger.exception(
            "Timed out deleting VolumeSnapshot %s/%s within %s seconds",
            namespace,
            name,
            time_limit,
        )
        raise


async def _cleanup_failed_snapshot_creation(
    *,
    namespace: str,
    snapshot_name: str,
    content_name: str | None,
    time_limit: float,
    poll_interval: float,
) -> None:
    logger.exception(
        "Failed to derive snapshot UUID for snapshot %s/%s; cleaning up created resources",
        namespace,
        snapshot_name,
    )
    try:
        await delete_branch_snapshot(
            name=snapshot_name,
            namespace=namespace,
            content_name=content_name,
            time_limit=time_limit,
            poll_interval=poll_interval,
        )
    except Exception:
        logger.exception(
            "Failed cleanup after snapshot UUID derivation failure for snapshot %s/%s",
            namespace,
            snapshot_name,
        )


async def branch_snapshots_used_size(
    backup_entries: list[BackupEntry],
) -> int:
    """
    Return total used size (bytes) for the provided backup entries.
    """
    snapshot_uuid_values = [entry.snapshot_uuid for entry in backup_entries if entry.snapshot_uuid is not None]
    if not snapshot_uuid_values:
        return 0

    try:
        snapshot_ids = [UUID(snapshot_uuid) for snapshot_uuid in snapshot_uuid_values]
    except ValueError as exc:
        raise VelaSimplyblockAPIError("Invalid snapshot UUID in backup entries") from exc

    try:
        async with create_simplyblock_api() as sb_api:
            snapshots = await sb_api.list_snapshots()
    except httpx.HTTPError as exc:
        logger.exception(
            "Failed to list Simplyblock snapshots for backup entries",
        )
        raise VelaSimplyblockAPIError("Failed to list Simplyblock snapshots") from exc

    used_size_by_id: dict[UUID, int] = {snapshot.id: snapshot.used_size for snapshot in snapshots}

    missing_ids = set(snapshot_ids) - set(used_size_by_id)
    if missing_ids:
        missing = ", ".join(str(snapshot_id) for snapshot_id in sorted(missing_ids, key=str))
        raise VelaSimplyblockAPIError(f"Missing snapshots in Simplyblock response: {missing}")

    return sum(used_size_by_id[snapshot_id] for snapshot_id in snapshot_ids)
