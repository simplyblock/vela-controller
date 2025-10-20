import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime

from aiohttp import ClientError
from kubernetes_asyncio import watch
from kubernetes_asyncio.client import CoreV1Api
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.client.models import CoreV1Event

from ...._util import quantity_to_bytes
from ....deployment.kubernetes._util import core_v1_client

logger = logging.getLogger(__name__)


FAILURE_PATTERN = re.compile(
    r"\b(resize|resizing|resized)\w*\b.*\b(fail|failure|failed|failing|error|err)\w*\b"
    r"|"
    r"\b(fail|failure|failed|failing|error|err)\w*\b.*\b(resize|resizing|resized)\w*\b",
    flags=re.IGNORECASE,
)

VOLUME_SERVICE_MAP = {
    "database": "database_disk_resize",
    "storage": "storage_api_disk_resize",
}

INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 60
PVC_EVENT_QUEUE_MAXSIZE = 2048
PVC_WORKER_POOL_SIZE = 4
PVC_WATCH_TIMEOUT_SECONDS = 60
PVC_QUEUE_PUT_TIMEOUT_SECONDS = 2


def normalize_iso_timestamp(value: datetime | None) -> str:
    """Return a UTC ISO-8601 string for Kubernetes event timestamps."""
    if value is None:
        value = datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def resource_from_pvc_name(name: str) -> str | None:
    """Infer which service a PVC belongs to based on the naming convention."""
    if name.endswith("-storage-pvc"):
        return "storage"
    if name.endswith("-pvc"):
        return "database"
    return None


def derive_status(reason: str | None, event_type: str | None, message: str | None) -> str | None:
    """Translate a Kubernetes Event into a high-level resize status."""
    normalized_reason = (reason or "").upper()
    normalized_message = message or ""
    if normalized_reason in {"RESIZING", "EXTERNALEXPANDING"}:
        return "RESIZING"
    if normalized_reason == "FILESYSTEMRESIZEREQUIRED":
        return "FILESYSTEM_RESIZE_PENDING"
    if normalized_reason in {"FILESYSTEMRESIZESUCCESSFUL", "RESIZEFINISHED"}:
        return "COMPLETED"
    if normalized_reason in {"VOLUMERESIZEFAILED", "FILESYSTEMRESIZEFAILED"}:
        return "FAILED"
    if (event_type or "").upper() == "WARNING" and FAILURE_PATTERN.search(normalized_message):
        return "FAILED"
    return None


async def fetch_pvc_capacity(core_v1: CoreV1Api, namespace: str, name: str) -> int | None:
    """Fetch the latest PVC capacity in bytes via the Kubernetes API."""
    try:
        pvc = await core_v1.read_namespaced_persistent_volume_claim(namespace=namespace, name=name)
    except (ApiException, ClientError):
        logger.exception("Failed to fetch PVC %s/%s for resize completion update", namespace, name)
        return None

    status_capacity = getattr(pvc.status, "capacity", None)
    if status_capacity:
        capacity = quantity_to_bytes(status_capacity.get("storage"))
        if capacity:
            return capacity

    resources = getattr(pvc.spec, "resources", None) if pvc.spec else None
    requests = getattr(resources, "requests", None)
    if requests:
        capacity = quantity_to_bytes(requests.get("storage"))
        if capacity:
            return capacity

    return None


EventHandler = Callable[[CoreV1Api, CoreV1Event], Awaitable[None]]


async def _enqueue_event(
    queue: asyncio.Queue[CoreV1Event | None],
    event: CoreV1Event,
    stop_event: asyncio.Event,
) -> bool:
    """Attempt to enqueue an event while honoring shutdown signals."""
    warned = False
    while True:
        if stop_event.is_set():
            return False
        try:
            await asyncio.wait_for(queue.put(event), timeout=PVC_QUEUE_PUT_TIMEOUT_SECONDS)
        except TimeoutError:
            if not warned:
                warned = True
                logger.warning(
                    "PVC resize monitor queue is saturated (%s/%s); delaying watcher",
                    queue.qsize(),
                    PVC_EVENT_QUEUE_MAXSIZE,
                )
            continue
        return True


async def _event_worker(
    worker_id: int,
    *,
    queue: asyncio.Queue[CoreV1Event | None],
    core_v1: CoreV1Api,
    handler: EventHandler,
    stop_event: asyncio.Event,
) -> None:
    """Drain the PVC event queue and invoke the handler in a dedicated task."""
    worker_name = f"pvc-resize-worker-{worker_id}"
    logger.debug("%s started", worker_name)
    try:
        while True:
            try:
                item = await queue.get()
            except asyncio.CancelledError:
                raise

            if item is None:
                queue.task_done()
                break

            try:
                await handler(core_v1, item)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s failed processing PVC event", worker_name)
            finally:
                queue.task_done()

            if stop_event.is_set() and queue.empty():
                logger.debug("%s draining queue and exiting", worker_name)
                break
    finally:
        logger.debug("%s stopped", worker_name)


async def _consume_event_stream(
    *,
    core_v1: CoreV1Api,
    field_selector: str,
    resource_version: str | None,
    stop_event: asyncio.Event,
    queue: asyncio.Queue[CoreV1Event | None],
) -> str | None:
    """Stream Kubernetes events until a shutdown signal or queue saturation occurs."""
    kube_watch = watch.Watch()
    try:
        async for item in kube_watch.stream(
            core_v1.list_event_for_all_namespaces,
            field_selector=field_selector,
            resource_version=resource_version,
            timeout_seconds=PVC_WATCH_TIMEOUT_SECONDS,
        ):
            if stop_event.is_set():
                break

            event: CoreV1Event = item["object"]
            metadata = getattr(event, "metadata", None)
            if metadata is not None:
                resource_version = getattr(metadata, "resource_version", resource_version)

            if not await _enqueue_event(queue, event, stop_event):
                break
        return resource_version
    finally:
        kube_watch.stop()


async def _handle_stream_error(
    exc: ApiException | ClientError,
    *,
    resource_version: str | None,
    backoff_seconds: int,
) -> tuple[str | None, int]:
    """Apply error handling and backoff policy for PVC event stream errors."""
    if isinstance(exc, ApiException) and exc.status == 410:
        logger.info("PVC resize monitor resource version expired; resyncing from latest")
        return "0", INITIAL_BACKOFF_SECONDS

    if isinstance(exc, ApiException):
        logger.warning("PVC resize monitor API error: %s", exc)
    else:
        logger.warning("PVC resize monitor stream client error: %s", exc)

    await asyncio.sleep(backoff_seconds)
    next_backoff = min(backoff_seconds * 2, MAX_BACKOFF_SECONDS)
    return resource_version, next_backoff


async def _watch_pvc_event_stream(
    *,
    core_v1: CoreV1Api,
    queue: asyncio.Queue[CoreV1Event | None],
    stop_event: asyncio.Event,
    field_selector: str,
) -> None:
    """Watch PVC events until stopped, applying backoff and error handling."""
    resource_version: str | None = "0"
    backoff_seconds = INITIAL_BACKOFF_SECONDS
    while not stop_event.is_set():
        try:
            resource_version = await _consume_event_stream(
                core_v1=core_v1,
                field_selector=field_selector,
                resource_version=resource_version,
                stop_event=stop_event,
                queue=queue,
            )
            backoff_seconds = INITIAL_BACKOFF_SECONDS
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        except (ApiException, ClientError) as exc:
            resource_version, backoff_seconds = await _handle_stream_error(
                exc,
                resource_version=resource_version,
                backoff_seconds=backoff_seconds,
            )


async def stream_pvc_events(stop_event: asyncio.Event, handler: EventHandler) -> None:
    """Watch PVC-related events and push them through a worker-backed queue."""
    field_selector = "involvedObject.kind=PersistentVolumeClaim"
    queue: asyncio.Queue[CoreV1Event | None] = asyncio.Queue(maxsize=PVC_EVENT_QUEUE_MAXSIZE)
    async with core_v1_client() as core_v1:
        workers = [
            asyncio.create_task(
                _event_worker(
                    worker_id,
                    queue=queue,
                    core_v1=core_v1,
                    handler=handler,
                    stop_event=stop_event,
                )
            )
            for worker_id in range(PVC_WORKER_POOL_SIZE)
        ]
        try:
            await _watch_pvc_event_stream(
                core_v1=core_v1,
                queue=queue,
                stop_event=stop_event,
                field_selector=field_selector,
            )
        finally:
            # Allow workers to finish in-flight items before shutting down.
            try:
                await asyncio.wait_for(queue.join(), timeout=PVC_WATCH_TIMEOUT_SECONDS)
            except TimeoutError:
                logger.warning(
                    "PVC resize monitor queue did not drain within %s seconds; cancelling workers",
                    PVC_WATCH_TIMEOUT_SECONDS,
                )
            for worker in workers:
                worker.cancel()
            for worker in workers:
                with suppress(asyncio.CancelledError):
                    await worker
