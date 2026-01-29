"""Resize monitor orchestration.

This module stitches together two complementary monitoring loops that ensure
branch resources reflect resize requests made through Vela:

PVC resize pipeline
-------------------
1. ``stream_pvc_events`` opens a long-lived Kubernetes watch filtered to PVC
   events. The watch feeds a bounded queue to decouple collection from
   processing, maintaining a resourceVersion for informer-like continuity and
   exponential backoff for resiliency.
2. A small worker pool (``_event_worker``) pulls events off the queue and calls
   ``_handle_pvc_event`` which enriches the raw Kubernetes event, fetching PVC
   capacity when necessary, and normalizes timestamps and statuses.
3. ``_apply_volume_status`` persists the synthesized status onto the owning
   ``Branch`` record, updating aggregate resize state and capacity fields so that
   API consumers observe consistent progress semantics.

Control surface
---------------
``run_resize_monitor`` drives both loops concurrently, coordinating shutdown via
``ResizeMonitor`` which offers a simple start/stop interface for long-lived
services. The design emphasizes:
* backpressure-aware queuing so surges of events do not starve handlers;
* idempotent database writes guarded by status transitions;
* graceful teardown of asyncio tasks to avoid leaking watches or sessions.
"""

import asyncio
import logging
from contextlib import suppress
from typing import Any, cast

from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.models import CoreV1Event
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from ulid import ULID

from ....api._util.resourcelimit import create_or_update_branch_provisioning
from ....api.db import engine
from ....deployment import deployment_branch
from ....exceptions import VelaDeploymentError, VelaKubernetesError
from ....models.branch import (
    RESIZE_STATUS_PRIORITY,
    Branch,
    BranchResizeStatus,
    aggregate_resize_statuses,
    should_transition_resize_status,
)
from ....models.resources import ResourceLimitsPublic
from ...health import collect_branch_service_health, derive_branch_status_from_services
from .pvc_resize import (
    INITIAL_BACKOFF_SECONDS,
    VOLUME_SERVICE_MAP,
    derive_status,
    fetch_pvc_capacity,
    normalize_iso_timestamp,
    resource_from_pvc_name,
    stream_pvc_events,
)

logger = logging.getLogger(__name__)


async def _apply_volume_status(
    *,
    branch_id: ULID,
    status: BranchResizeStatus | None,
    event_payload: dict[str, str],
    resource: str | None,
    capacity: int | None,
) -> None:
    """Persist the PVC resize status and size information on the branch record."""
    from sqlalchemy.exc import NoResultFound

    async with AsyncSession(engine) as session:
        statement = select(Branch).where(Branch.id == branch_id)
        result = await session.exec(statement)
        try:
            branch = result.one()
        except NoResultFound:
            return

        service_key = VOLUME_SERVICE_MAP.get(resource) if resource else None
        statuses = dict(branch.resize_statuses or {})
        status_updated = False

        if status is not None and service_key:
            current_entry = statuses.get(service_key)
            current_status_value = None
            if isinstance(current_entry, dict):
                current_status_value = current_entry.get("status")
            if isinstance(current_status_value, str) and current_status_value in RESIZE_STATUS_PRIORITY:
                current_status = cast("BranchResizeStatus", current_status_value)
            else:
                current_status = None

            if should_transition_resize_status(current_status, status):
                statuses[service_key] = {
                    "status": status,
                    "timestamp": event_payload["timestamp"],
                }
                branch.resize_statuses = statuses
                branch.resize_status = aggregate_resize_statuses(statuses)
                await set_branch_status(branch.resize_status, branch)
                status_updated = True
        elif status is not None and should_transition_resize_status(branch.resize_status, status):
            branch.resize_status = status
            await set_branch_status(branch.resize_status, branch)
            status_updated = True

        if status_updated and status == "COMPLETED" and capacity is not None:
            if resource == "storage":
                await create_or_update_branch_provisioning(
                    session,
                    branch,
                    ResourceLimitsPublic(storage_size=capacity),
                    commit=False,
                )
                branch.storage_size = capacity
            elif resource == "database":
                await create_or_update_branch_provisioning(
                    session,
                    branch,
                    ResourceLimitsPublic(database_size=capacity),
                    commit=False,
                )
                branch.database_size = capacity

        await session.commit()


async def set_branch_status(status: BranchResizeStatus, branch: Branch) -> None:
    if status not in {"FAILED", "COMPLETED"}:
        return

    service_status = await collect_branch_service_health(branch.id)
    branch.set_status(
        derive_branch_status_from_services(
            service_status,
            storage_enabled=branch.enable_file_storage,
        )
    )


async def _handle_pvc_event(core_v1: CoreV1Api, event: CoreV1Event) -> None:
    """Map a raw Kubernetes event into branch status updates and capacity changes."""
    ref = event.involved_object
    namespace = getattr(ref, "namespace", None)
    name = getattr(ref, "name", None)
    if not namespace or not name:
        return
    try:
        branch_id = deployment_branch(namespace)
    except VelaDeploymentError:
        logger.debug("Ignoring resize event for namespace %s (not a branch namespace)", namespace)
        return
    resource = resource_from_pvc_name(name)
    status = derive_status(event.reason, event.type, event.message)
    capacity = None
    if status == "COMPLETED":
        capacity = await fetch_pvc_capacity(core_v1, namespace, name)

    status = cast("BranchResizeStatus", status)
    timestamp_source = (
        event.last_timestamp or event.event_time or (event.metadata.creation_timestamp if event.metadata else None)
    )
    payload = {
        "timestamp": normalize_iso_timestamp(timestamp_source),
        "type": event.reason or "",
        "message": event.message or "",
    }
    await _apply_volume_status(
        branch_id=branch_id,
        status=status,
        event_payload=payload,
        resource=resource,
        capacity=capacity,
    )


async def run_resize_monitor(stop_event: asyncio.Event) -> None:
    """Drive PVC resize monitors until the caller signals shutdown."""
    while not stop_event.is_set():
        try:
            await stream_pvc_events(stop_event, _handle_pvc_event)
        except VelaKubernetesError as exc:
            logger.warning("PVC resize monitor awaiting Kubernetes configuration: %s", exc)
            await asyncio.sleep(INITIAL_BACKOFF_SECONDS)
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("PVC resize monitor unexpected failure; retrying")
            await asyncio.sleep(INITIAL_BACKOFF_SECONDS)


class ResizeMonitor:
    """Lightweight wrapper for starting and stopping the resize monitor loop."""

    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    def start(self) -> None:
        """Ensure the background resize monitor task is running."""
        if self._task is None:
            self._task = asyncio.create_task(run_resize_monitor(self._stop))

    async def stop(self) -> None:
        """Shut down the background task and reset internal state."""
        if self._task is None:
            return
        self._stop.set()
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        self._stop = asyncio.Event()


__all__ = ["ResizeMonitor", "run_resize_monitor"]
