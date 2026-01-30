from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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
from ..exceptions import VelaKubernetesError, VelaSnapshotTimeoutError

if TYPE_CHECKING:
    from ulid import ULID

logger = logging.getLogger(__name__)

SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
SNAPSHOT_POLL_INTERVAL_SEC = int(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))

_K8S_NAME_MAX_LENGTH = 63


@dataclass(frozen=True)
class SnapshotDetails:
    name: str
    namespace: str
    content_name: str | None
    size_bytes: int | None
    used_size_bytes: int | None


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
    content_name = status.get("boundVolumeSnapshotContentName")
    size_bytes = quantity_to_bytes(status.get("restoreSize"))

    used_size_bytes = None
    if content_name:
        try:
            used_size_bytes = await _snapshot_used_size(content_name)
        except (ValueError, VelaKubernetesError, httpx.HTTPError):
            logger.exception("Failed to derive used_size for snapshot %s/%s", namespace, snapshot_name)
    logger.info(
        "VolumeSnapshot %s/%s ready (content=%s size_bytes=%s used_size_bytes=%s)",
        namespace,
        snapshot_name,
        content_name,
        size_bytes,
        used_size_bytes,
    )

    return SnapshotDetails(
        name=snapshot_name,
        namespace=namespace,
        content_name=content_name,
        size_bytes=size_bytes,
        used_size_bytes=used_size_bytes,
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


async def _snapshot_used_size(content_name: str | None) -> int:
    if content_name is None:
        raise ValueError("vollume snapshot content_name empty")

    content = await read_snapshot_content(content_name)
    snapshot_handle = (content or {}).get("status", {}).get("snapshotHandle")
    if not snapshot_handle:
        raise ValueError("snapshotHandle missing")

    snapshot_uuid = snapshot_handle.split(":")[1]

    async with create_simplyblock_api() as sb_api:
        details = await sb_api.snapshot_details(snapshot_uuid)

    used_size: Any = details.get("used_size")
    if not isinstance(used_size, (int, float)):
        raise ValueError("used_size missing or not numeric")
    return int(used_size)
