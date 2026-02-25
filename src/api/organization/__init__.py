from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlmodel import col, func, select
from ulid import ULID

from ...deployment import delete_deployment
from ...models.audit import OrganizationAuditLog
from ...models.organization import Organization, OrganizationCreate, OrganizationUpdate
from ...models.resources import ResourceTypePublic, ResourceUsageMinute
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from .._util.resourcelimit import initialize_organization_resource_limits
from .._util.role import create_organization_admin_role
from ..auth import authenticated_user
from ..dependencies import AuthUserDep, OrganizationDep, SessionDep
from .member import api as member_api
from .project import api as project_api
from .role import RoleUserLink
from .role import api as role_api

api = APIRouter(dependencies=[Depends(authenticated_user)], tags=["organization"])


@api.get(
    "/",
    name="organizations:list",
    responses={401: Unauthenticated},
)
async def list_(user: AuthUserDep) -> Sequence[Organization]:
    return await user.awaitable_attrs.organizations


_links = {
    "detail": {
        "operationId": "organizations:detail",
        "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "update": {
        "operationId": "organizations:update",
        "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "delete": {
        "operationId": "organizations:delete",
        "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "create_project": {
        "operationId": "organizations:projects:create",
        "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "list_projects": {
        "operationId": "organizations:projects:list",
        "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"},
    },
}


@api.post(
    "/",
    name="organizations:create",
    status_code=201,
    response_model=Organization | None,
    responses={
        201: {
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": _links,
        },
        401: Unauthenticated,
        409: Conflict,
    },
)
async def create(
    session: SessionDep,
    request: Request,
    parameters: OrganizationCreate,
    user: AuthUserDep,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    entity = Organization(**parameters.model_dump(), users=[user])
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as e:
        error = str(e)
        if ("asyncpg.exceptions.UniqueViolationError" not in error) or ("organization_name_key" not in error):
            raise
        raise HTTPException(409, f"Organization {parameters.name} already exists") from e
    await session.refresh(entity)

    # Create the organization admin role
    admin_role = await create_organization_admin_role(session, entity)

    # Assign the organization admin role to the user
    await session.refresh(user)
    link = RoleUserLink(role_id=admin_role.id, user_id=user.id, organization_id=entity.id)
    session.add(link)
    await session.commit()
    await session.refresh(entity)

    # Set up initial organization resource limits
    await initialize_organization_resource_limits(session, entity)

    entity_url = url_path_for(request, "organizations:detail", organization_id=entity.id)
    return JSONResponse(
        content=entity.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


async def _check_user_access(user: AuthUserDep, organization: OrganizationDep):
    if organization.require_mfa and not user.token.mfa():
        raise HTTPException(401, detail="This operation requires multi-factor authentication")

    if user not in await organization.awaitable_attrs.users:
        raise HTTPException(403, detail="Unauthorized access")


instance_api = APIRouter(
    prefix="/{organization_id}",
    dependencies=[Depends(_check_user_access)],
    tags=["organization"],
)


@instance_api.get(
    "/",
    name="organizations:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(organization: OrganizationDep) -> Organization:
    return organization


@instance_api.put(
    "/",
    name="organizations:update",
    status_code=204,
    responses={
        204: {
            "content": None,
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": _links,
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def update(request: Request, session: SessionDep, organization: OrganizationDep, parameters: OrganizationUpdate):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert hasattr(organization, key)
        setattr(organization, key, value)

    try:
        await session.commit()
    except IntegrityError as e:
        raise HTTPException(409, f"Organization {parameters.name} already exists") from e

    # Refer to potentially updated location
    return Response(
        status_code=204,
        headers={
            "Location": url_path_for(
                request,
                "organizations:detail",
                organization_id=await organization.awaitable_attrs.id,
            ),
        },
    )


@instance_api.delete(
    "/",
    name="organizations:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(session: SessionDep, organization: OrganizationDep):
    projects = await organization.awaitable_attrs.projects
    for project in projects:
        await session.refresh(project, ["branches"])
        branches = await project.awaitable_attrs.branches
        for branch in branches:
            await delete_deployment(branch.id)

    await session.delete(organization)
    await session.commit()
    return Response(status_code=204)


@instance_api.get(
    "/audit",
    name="organizations:audits:list",
    status_code=200,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
def list_audits(
    _from: Annotated[datetime, Query(alias="from")],
    _to: Annotated[datetime, Query(alias="to")],
) -> OrganizationAuditLog:
    return OrganizationAuditLog(result=[], retention_period=0)


class Metering(BaseModel):
    organization_id: ULID
    project_id: ULID
    branch_id: ULID
    amount: Decimal
    type: ResourceTypePublic


@instance_api.get("/metering/")
async def metering(
    session: SessionDep, organization: OrganizationDep, start: datetime | None = None, end: datetime | None = None
) -> list[Metering]:
    usage_cte = select(  # type: ignore[call-overload]
        ResourceUsageMinute.id,
        ResourceUsageMinute.ts_minute,
        ResourceUsageMinute.org_id,
        ResourceUsageMinute.original_project_id,
        ResourceUsageMinute.original_branch_id,
        ResourceUsageMinute.resource,
        ResourceUsageMinute.amount,
    )

    if start is not None:
        usage_cte = usage_cte.where(ResourceUsageMinute.ts_minute >= func.date_trunc("minute", start))

    if end is not None:
        usage_cte = usage_cte.where(ResourceUsageMinute.ts_minute <= func.date_trunc("minute", end))

    usage_cte = usage_cte.order_by(
        col(ResourceUsageMinute.original_project_id).desc(),
        col(ResourceUsageMinute.original_branch_id).desc(),
        col(ResourceUsageMinute.resource).desc(),
        col(ResourceUsageMinute.ts_minute).desc(),
    ).cte("usage")

    statement = (
        select(  # type: ignore[call-overload]
            usage_cte.c.org_id.label("organization_id"),
            usage_cte.c.original_project_id.label("project_id"),
            usage_cte.c.original_branch_id.label("branch_id"),
            usage_cte.c.resource.label("type"),
            func.sum(usage_cte.c.amount).label("amount"),
        )
        .where(usage_cte.c.org_id == organization.id)
        .group_by(
            usage_cte.c.org_id, usage_cte.c.original_project_id, usage_cte.c.original_branch_id, usage_cte.c.resource
        )
    )
    return (await session.exec(statement)).all()


instance_api.include_router(project_api, prefix="/projects")
instance_api.include_router(member_api, prefix="/members")
instance_api.include_router(role_api, prefix="/roles")
api.include_router(instance_api)
