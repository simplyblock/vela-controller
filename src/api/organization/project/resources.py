from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter

from ....models.resources import EntityType, ResourceLimitsPublic
from ..._util import Forbidden, NotFound, Unauthenticated
from ..._util.resourcelimit import Limits, Resources, project_allocations, project_available, project_limits
from ...dependencies import ProjectDep, SessionDep

api = APIRouter()


@api.get(
    "/limits/",
    name="organizations:projects:resources:limits",
    response_model=Limits,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def limits(project: ProjectDep):
    limits = await project_limits(project)
    response_data = TypeAdapter(Limits).dump_python(
        limits,
        exclude=None
        if (await project.storage_enabled)
        else {"total": {"storage_size"}, "per_branch": {"storage_size"}},
    )
    return JSONResponse(content=response_data)


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
    "/consumed/",
    name="organizations:projects:resources:allocated",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def allocated(session: SessionDep, project: ProjectDep):
    allocations = await project_allocations(session, project)
    response_data = TypeAdapter(Resources).dump_python(
        allocations, exclude=None if (await project.storage_enabled) else {"storage_size"}
    )
    return JSONResponse(content=response_data)


@api.get(
    "/available/",
    name="organizations:projects:resources:available",
    response_model=ResourceLimitsPublic,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def available(session: SessionDep, project: ProjectDep):
    available = await project_available(session, project)
    response_data = TypeAdapter(Resources).dump_python(
        available, exclude=None if (await project.storage_enabled) else {"storage_size"}
    )
    return JSONResponse(content=response_data)
