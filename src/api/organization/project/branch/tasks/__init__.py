from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ....._util import Forbidden, NotFound, Unauthenticated
from .....dependencies import BranchDep, OrganizationDep, ProjectDep
from ._control import _CONTROL_TO_POWER_STATE as _CONTROL_TO_POWER_STATE
from ._control import dispatch_control as dispatch_control
from ._control import get_control_in_progress_status as get_control_in_progress_status
from ._control import perform_control
from ._delete import finalize_delete
from ._resize import dispatch_resize as dispatch_resize
from ._resize import finalize_resize

api = APIRouter(tags=["branch"])

TaskType = Literal["control", "delete", "resize"]

_CELERY_STATE_TO_STATUS: dict[str, str] = {
    "PENDING": "PENDING",
    "STARTED": "STARTED",
    "SUCCESS": "COMPLETED",
    "FAILURE": "FAILED",
    "REVOKED": "FAILED",
}


class BranchTaskPublic(BaseModel):
    id: UUID
    task_type: str
    status: str
    parameters: dict
    result: Any | None
    error: str | None
    date_done: datetime | None


def _build_task_public(task_id: UUID, task_type: TaskType) -> BranchTaskPublic:
    tasks = {
        "control": perform_control,
        "delete": finalize_delete,
        "resize": finalize_resize,
    }
    result = tasks[task_type].AsyncResult(str(task_id))

    state = result.state
    status = _CELERY_STATE_TO_STATUS.get(state, state)
    kwargs: dict = result.kwargs or {}
    task_type = task_type if task_type != "control" else kwargs["action"]
    parameters = {k: v for k, v in kwargs.items() if k not in {"branch_id", "action"}}
    return BranchTaskPublic(
        id=task_id,
        task_type=task_type,
        status=status,
        parameters=parameters,
        result=result.result if state == "SUCCESS" else None,
        error=str(result.traceback) if state == "FAILURE" and result.traceback else None,
        date_done=result.date_done,
    )


@api.get(
    "/",
    name="organizations:projects:branch:tasks:list",
    response_model=list[BranchTaskPublic],
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_tasks(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> list[BranchTaskPublic]:
    tasks: list[tuple[UUID | None, TaskType]] = [
        (branch.control_task_id, "control"),
        (branch.delete_task_id, "delete"),
        (branch.resize_task_id, "resize"),
    ]
    return [_build_task_public(task_id, task_type) for task_id, task_type in tasks if task_id is not None]


@api.get(
    "/{task_id}",
    name="organizations:projects:branch:tasks:detail",
    response_model=BranchTaskPublic,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def get_task(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
    task_id: UUID,
) -> BranchTaskPublic:
    if branch.resize_task_id == task_id:
        return _build_task_public(task_id, "resize")
    if branch.control_task_id == task_id:
        return _build_task_public(task_id, "control")
    if branch.delete_task_id == task_id:
        return _build_task_public(task_id, "delete")
    raise HTTPException(status_code=404, detail="Task not found")
