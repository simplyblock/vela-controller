from fastapi import APIRouter, Response

from ....models.resources import EntityType
from ..._util import Forbidden, NotFound, Unauthenticated
from ..._util.resourcelimit import (
    Limits,
    Resources,
    project_allocations,
    project_available,
    project_branch_maxima,
    project_limits,
)
from ...dependencies import ProjectDep, SessionDep

api = APIRouter()


@api.get(
    "/limits/",
    name="organizations:projects:resources:limits",
    response_model=Limits,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def limits(project: ProjectDep) -> Limits:
    return await project_limits(project)


@api.put(
    "/limits/",
    name="organizations:projects:resources:update-limits",
    status_code=204,
    responses={204: {"content": None}, 401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update_limits(session: SessionDep, project: ProjectDep, limits: Limits):
    _ = await project.awaitable_attrs.limits  # Ensure status-quo is known
    project.limits = []
    await session.flush()  # delete previous limits
    project.limits = limits.to_database(EntityType.project)
    await session.commit()
    return Response(status_code=204)


@api.get(
    "/allocations/",
    name="organizations:projects:resources:allocations",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def allocations(session: SessionDep, project: ProjectDep) -> Resources:
    return await project_allocations(session, project)


@api.get(
    "/available/",
    name="organizations:projects:resources:available",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def available(session: SessionDep, project: ProjectDep) -> Resources:
    return await project_available(session, project)


@api.get(
    "/branch-maxima/",
    name="organizations:projects:resources:branch-maxima",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def branch_maxima(session: SessionDep, project: ProjectDep) -> Resources:
    return await project_branch_maxima(session, project)
