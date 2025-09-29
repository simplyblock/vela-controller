from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from ...deployment import delete_deployment
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..auth import AuthUserDep, authenticated_user
from ..db import SessionDep
from ..models.audit import OrganizationAuditLog
from ..models.branch import Branch
from ..models.organization import Organization, OrganizationCreate, OrganizationDep, OrganizationUpdate
from .member import api as member_api
from .project import api as project_api
from .project.branch import api as branch_api
from .role import api as role_api

api = APIRouter(dependencies=[Depends(authenticated_user)])


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
        if ("asyncpg.exceptions.UniqueViolationError" not in error) or ("organization_slug_key" not in error):
            raise
        raise HTTPException(409, f"Organization {parameters.name} already exists") from e
    await session.refresh(entity)
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
    for project in await organization.awaitable_attrs.projects:
        delete_deployment(project.dbid(), Branch.DEFAULT_SLUG)

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


instance_api.include_router(project_api, prefix="/projects")
instance_api.include_router(member_api, prefix="/members")
instance_api.include_router(role_api, prefix="/roles")
instance_api.include_router(branch_api, prefix="/branches")
api.include_router(instance_api)
