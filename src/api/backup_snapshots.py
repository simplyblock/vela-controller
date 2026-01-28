from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from .._util import Identifier, quantity_to_bytes
from .._util.backup_config import (
    SNAPSHOT_POLL_INTERVAL_SEC,
    SNAPSHOT_TIMEOUT_SEC,
    VOLUME_SNAPSHOT_CLASS,
)
from ..deployment import (
    AUTOSCALER_PVC_SUFFIX,
    get_autoscaler_vm_identity,
)
from ..deployment.kubernetes.snapshot import (
    create_snapshot_from_pvc,
    ensure_snapshot_absent,
    ensure_snapshot_content_absent,
    read_snapshot,
    wait_snapshot_ready,
)
from ..exceptions import VelaSnapshotTimeoutError

if TYPE_CHECKING:
    from ulid import ULID

    from ..models.backups import BackupEntry

logger = logging.getLogger(__name__)

_K8S_NAME_MAX_LENGTH = 63
DEFAULT_SNAPSHOT_TIMEOUT_SEC = float(SNAPSHOT_TIMEOUT_SEC)
DEFAULT_SNAPSHOT_POLL_INTERVAL_SEC = float(SNAPSHOT_POLL_INTERVAL_SEC)


class SnapshotMetadata(BaseModel):
    name: str = Field(..., min_length=1)
    namespace: str = Field(..., min_length=1)
    # content_name stays optional because there are runtime scenarios where the
    # VolumeSnapshotContent hasn’t been bound yet
    content_name: str | None


def build_snapshot_metadata(backup: BackupEntry) -> SnapshotMetadata | None:
    name = backup.snapshot_name
    namespace = backup.snapshot_namespace
    if not name or not namespace:
        logger.debug(
            "Skipping metadata for missing snapshot identifiers (name=%r namespace=%r)",
            name,
            namespace,
        )
        return None
    return SnapshotMetadata(
        name=name,
        namespace=namespace,
        content_name=backup.snapshot_content_name,
    )


@dataclass(frozen=True)
class SnapshotDetails:
    name: str
    namespace: str
    content_name: str | None
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


async def _create_snapshot_from_pvc(
    *,
    namespace: str,
    pvc_name: str,
    backup_id: ULID,
    snapshot_class: str,
    label: str,
    poll_interval: float,
    time_limit: float,
) -> SnapshotDetails:
    snapshot_name = _build_snapshot_name(label=label, backup_id=backup_id)
    logger.info("Creating VolumeSnapshot %s/%s for branch PVC %s", namespace, snapshot_name, pvc_name)
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
            "Timed out creating VolumeSnapshot %s/%s for PVC %s within %s seconds",
            namespace,
            snapshot_name,
            pvc_name,
            time_limit,
        )
        raise VelaSnapshotTimeoutError(
            f"Timed out creating VolumeSnapshot {namespace}/{snapshot_name} for namespace {namespace}"
        ) from exc

    status = snapshot.get("status") or {}
    content_name = status.get("boundVolumeSnapshotContentName")
    size_bytes = quantity_to_bytes(status.get("restoreSize"))
    logger.info(
        "VolumeSnapshot %s/%s ready (content=%s size_bytes=%s)",
        namespace,
        snapshot_name,
        content_name,
        size_bytes,
    )

    return SnapshotDetails(
        name=snapshot_name,
        namespace=namespace,
        content_name=content_name,
        size_bytes=size_bytes,
    )


async def create_branch_db_snapshot(
    branch_id: Identifier,
    *,
    backup_id: ULID,
    snapshot_class: str = VOLUME_SNAPSHOT_CLASS,
    poll_interval: float = DEFAULT_SNAPSHOT_POLL_INTERVAL_SEC,
    label: str,
    time_limit: float = DEFAULT_SNAPSHOT_TIMEOUT_SEC,
) -> SnapshotDetails:
    namespace, autoscaler_vm_name = get_autoscaler_vm_identity(branch_id)
    pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"
    return await _create_snapshot_from_pvc(
        namespace=namespace,
        pvc_name=pvc_name,
        backup_id=backup_id,
        snapshot_class=snapshot_class,
        poll_interval=poll_interval,
        label=label,
        time_limit=time_limit,
    )


async def delete_snapshot(
    metadata: SnapshotMetadata,
    *,
    time_limit: float = DEFAULT_SNAPSHOT_TIMEOUT_SEC,
    poll_interval: float = DEFAULT_SNAPSHOT_POLL_INTERVAL_SEC,
) -> None:
    name = metadata.name
    namespace = metadata.namespace
    content_name = metadata.content_name
    try:
        async with asyncio.timeout(time_limit):
            snapshot = await read_snapshot(namespace, name)
            if snapshot is not None:
                status = snapshot.get("status") or {}
                content_name = content_name or status.get("boundVolumeSnapshotContentName")
                logger.info("Deleting VolumeSnapshot %s/%s", namespace, name)
                await ensure_snapshot_absent(
                    namespace,
                    name,
                    timeout=time_limit,
                    poll_interval=poll_interval,
                )
            else:
                logger.info("VolumeSnapshot %s/%s already absent", namespace, name)

            if content_name:
                logger.info("Ensuring VolumeSnapshotContent %s is absent", content_name)
                await ensure_snapshot_content_absent(
                    content_name,
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
