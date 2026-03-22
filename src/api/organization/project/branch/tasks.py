"""Branch task list/detail endpoints.

Exposes Celery task state (currently resize only) under:
  GET .../branches/{branch_id}/tasks
  GET .../branches/{branch_id}/tasks/{task_id}
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .....deployment.resize import get_resize_task_result
from ...._util import Forbidden, NotFound, Unauthenticated
from ....dependencies import BranchDep, OrganizationDep, ProjectDep

task_api = APIRouter(tags=["branch"])

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


def _build_task_public(task_id: UUID) -> BranchTaskPublic:
    result = get_resize_task_result(task_id)
    state = result.state
    status = _CELERY_STATE_TO_STATUS.get(state, state)
    kwargs: dict = result.kwargs or {}
    return BranchTaskPublic(
        id=task_id,
        task_type="resize",
        status=status,
        parameters=kwargs.get("effective_parameters", {}),
        result=result.result if state == "SUCCESS" else None,
        error=str(result.traceback) if state == "FAILURE" and result.traceback else None,
        date_done=result.date_done,
    )


@task_api.get(
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
    if branch.resize_task_id is None:
        return []
    return [_build_task_public(branch.resize_task_id)]


@task_api.get(
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
    if branch.resize_task_id != task_id:
        raise HTTPException(status_code=404, detail="Task not found")
    return _build_task_public(task_id)
