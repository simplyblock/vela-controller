from fastapi import APIRouter, Response

from ...models.resources import EntityType
from .._util import Forbidden, NotFound, Unauthenticated
from .._util.resourcelimit import (
    Limits,
    Resources,
    organization_allocations,
    organization_available,
    organization_limits,
)
from ..dependencies import OrganizationDep, SessionDep

api = APIRouter()


@api.get(
    "/limits/",
    name="organizations:resources:limits",
    response_model=Limits,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def limits(organization: OrganizationDep) -> Limits:
    return await organization_limits(organization)


@api.put(
    "/limits/",
    name="organizations:resources:update-limits",
    status_code=204,
    responses={204: {"content": None}, 401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update_limits(session: SessionDep, organization: OrganizationDep, limits: Limits):
    _ = await organization.awaitable_attrs.limits  # Ensure status-quo is known
    organization.limits = []
    await session.flush()  # delete previous limits
    organization.limits = limits.to_database(EntityType.org)
    await session.commit()
    return Response(status_code=204)


@api.get(
    "/allocations/",
    name="organizations:resources:allocations",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def allocations(session: SessionDep, organization: OrganizationDep) -> Resources:
    return await organization_allocations(session, organization)


@api.get(
    "/available/",
    name="organizations:resources:available",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def available(session: SessionDep, organization: OrganizationDep) -> Resources:
    return await organization_available(session, organization)
