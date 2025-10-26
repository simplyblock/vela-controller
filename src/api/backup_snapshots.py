from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .._util import Identifier, quantity_to_bytes
from ..deployment import DATABASE_PVC_SUFFIX, deployment_namespace
from ..deployment.kubernetes.snapshot import (
    create_snapshot_from_pvc,
    ensure_snapshot_absent,
    ensure_snapshot_content_absent,
    read_snapshot,
    wait_snapshot_ready,
)
from ..deployment.settings import settings as deployment_settings

if TYPE_CHECKING:
    from ulid import ULID

logger = logging.getLogger(__name__)

_K8S_NAME_MAX_LENGTH = 63


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


async def create_branch_snapshot(
    branch_id: Identifier,
    *,
    backup_id: ULID,
    snapshot_class: str,
    poll_interval: float,
    label: str,
    time_limit: float,
) -> SnapshotDetails:
    namespace = deployment_namespace(branch_id)
    pvc_name = f"{deployment_settings.deployment_release_name}{DATABASE_PVC_SUFFIX}"
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
    except TimeoutError:
        logger.exception(
            "Timed out creating VolumeSnapshot %s/%s for branch %s within %s seconds",
            namespace,
            snapshot_name,
            branch_id,
            time_limit,
        )
        raise

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


async def delete_branch_snapshot(
    *,
    name: str | None,
    namespace: str | None,
    content_name: str | None,
    time_limit: float,
    poll_interval: float,
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
