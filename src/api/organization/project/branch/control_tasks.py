"""Celery task for branch start/stop/pause/resume lifecycle management.

For start/resume the task polls the health monitor until the branch is
ACTIVE_HEALTHY, then writes that status and clears itself from the branch.
For stop/pause the task waits for the NeonVM phase to reach stopped/succeeded.
While the task is running any status query returns the in-progress shadow
status (STARTING/RESUMING/STOPPING/PAUSING) instead of the DB value.
"""

import asyncio
import logging
from typing import Any
from uuid import UUID

from billiard.exceptions import SoftTimeLimitExceeded
from sqlalchemy.exc import NoResultFound
from ulid import ULID

from .....database import AsyncSessionLocal
from .....deployment import get_autoscaler_vm_identity
from .....deployment.health import query_deployment_status
from .....deployment.kubernetes.neonvm import Phase, PowerState, get_neon_vm, set_virtualmachine_power_state
from .....models.branch import BranchServiceStatus
from .....models.branch import lookup as branch_lookup
from .....worker import app

logger = logging.getLogger(__name__)

_CONTROL_TO_POWER_STATE: dict[str, PowerState] = {
    "pause": "Stopped",
    "resume": "Running",
    "start": "Running",
    "stop": "Stopped",
}

_CONTROL_TRANSITION_INITIAL: dict[str, BranchServiceStatus] = {
    "pause": BranchServiceStatus.PAUSING,
    "resume": BranchServiceStatus.RESUMING,
    "start": BranchServiceStatus.STARTING,
    "stop": BranchServiceStatus.STOPPING,
}

_CONTROL_TRANSITION_FINAL: dict[str, BranchServiceStatus] = {
    "pause": BranchServiceStatus.PAUSED,
    "stop": BranchServiceStatus.STOPPED,
}

_DESIRED_PHASES: dict[str, set[Phase]] = {
    "stop": {Phase.stopped, Phase.succeeded},
    "pause": {Phase.stopped, Phase.succeeded},
}

_POLL_INTERVAL_SEC = 5


class _TaskRequest:
    """Minimal Celery request stub used to pre-populate the result backend.

    Celery only stores task kwargs in the backend when ``store_result`` is
    called with a request object (requires ``result_extended = True``).  By
    constructing this stub at dispatch time we make ``AsyncResult.kwargs``
    readable immediately — before the worker has even picked up the message.
    """

    __slots__ = ("id", "task", "args", "kwargs")

    def __init__(self, task_id: str, kwargs: dict[str, Any]) -> None:
        self.id = task_id
        self.task: str = perform_control.name
        self.args: tuple[()] = ()
        self.kwargs = kwargs


async def _write_branch_status(
    session: object,
    ulid: ULID,
    branch_id: str,
    action: str,
    status: BranchServiceStatus,
) -> None:
    try:
        branch = await branch_lookup(session, ulid)  # type: ignore[arg-type]
    except NoResultFound:
        logger.error("Branch %s not found after control action %s", branch_id, action)
        return
    branch.set_status(status)
    branch.control_task_id = None
    await session.commit()  # type: ignore[union-attr,attr-defined]


async def _async_perform_control(branch_id: str, action: str) -> dict:
    ulid = ULID.from_str(branch_id)
    namespace, name = get_autoscaler_vm_identity(ulid)

    async with AsyncSessionLocal() as session:
        try:
            await set_virtualmachine_power_state(namespace, name, _CONTROL_TO_POWER_STATE[action])

            if action in ("start", "resume"):
                while True:
                    current = await query_deployment_status(namespace, name)
                    if current == BranchServiceStatus.ACTIVE_HEALTHY:
                        break
                    if current == BranchServiceStatus.ERROR:
                        raise RuntimeError(f"Branch {branch_id!r} entered ERROR state during {action!r}")
                    await asyncio.sleep(_POLL_INTERVAL_SEC)
                final_status = BranchServiceStatus.ACTIVE_HEALTHY
            else:
                # Poll NeonVM directly until the VM reaches the stopped phase.
                desired_phases = _DESIRED_PHASES[action]
                while True:
                    try:
                        vm = await get_neon_vm(namespace, name)
                        if vm.status is not None and vm.status.phase in desired_phases:
                            break
                    except RuntimeError:
                        # VM resource may be transiently absent (404) while NeonVM
                        # reconciles the power-state change; keep polling.
                        logger.debug("VM %r transiently unavailable during %r, retrying", name, action)
                    await asyncio.sleep(_POLL_INTERVAL_SEC)
                final_status = _CONTROL_TRANSITION_FINAL[action]

            await _write_branch_status(session, ulid, branch_id, action, final_status)
        except Exception:  # noqa: BLE001
            await session.rollback()
            await _write_branch_status(session, ulid, branch_id, action, BranchServiceStatus.ERROR)

    return {"action": action}


@app.task(name="simplyblock.vela.branch.control", soft_time_limit=60, time_limit=90)
def perform_control(branch_id: str, action: str) -> dict:
    """Patch VM power state, wait for desired state, update branch status."""
    loop = asyncio.new_event_loop()
    try:
        task = loop.create_task(_async_perform_control(branch_id, action))
        try:
            return loop.run_until_complete(task)
        except SoftTimeLimitExceeded:
            task.cancel()
            return loop.run_until_complete(task)  # Assume the task is well-behaved and cleans up
    finally:
        loop.close()


def get_control_in_progress_status(task_id: UUID) -> BranchServiceStatus | None:
    """Return the in-progress status for a running control task, or None."""
    result = perform_control.AsyncResult(str(task_id))
    if result.ready():
        return None
    action = (result.kwargs or {}).get("action")
    if action in _CONTROL_TRANSITION_INITIAL:
        return _CONTROL_TRANSITION_INITIAL[action]
    return None


def dispatch_control(branch_id: str, action: str) -> UUID:
    """Dispatch perform_control asynchronously; return the Celery task UUID.

    The task kwargs are pre-stored in the result backend immediately so that
    ``AsyncResult.kwargs`` (and therefore ``get_control_in_progress_status``)
    works even before the worker picks up the message.
    """
    kwargs = {"branch_id": branch_id, "action": action}
    result = perform_control.apply_async(kwargs=kwargs)
    task_id = str(result.id)
    app.backend.store_result(task_id, None, "PENDING", request=_TaskRequest(task_id, kwargs))
    return UUID(task_id)
