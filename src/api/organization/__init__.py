from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlmodel.ext.asyncio.session import AsyncSession

from ...deployment import delete_deployment
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..auth import AuthUserDep, authenticated_user
from ..db import get_db
from ..models.audit import OrganizationAuditLog
from ..models.organization import Organization, OrganizationCreate, OrganizationDep, OrganizationUpdate
from .member import api as member_api
from .role import api as role_api

# --- Dependencies ---
SessionDep = Annotated[AsyncSession, Depends(get_db)]
UserDep = Annotated[AuthUserDep, Depends(authenticated_user)]

# --- Routers ---
api = APIRouter(prefix="/organizations")
instance_api = APIRouter(
    prefix="/organizations/{organization_id}",
    tags=["organization"],
)

# --- Helper ---
async def _check_user_access(user: UserDep, organization: OrganizationDep):
    if organization.require_mfa and not user.token.mfa():
        raise HTTPException(401, detail="This operation requires multi-factor authentication")
    if user not in await organization.awaitable_attrs.users:
        raise HTTPException(403, detail="Unauthorized access")

instance_api.dependencies.append(Depends(_check_user_access))

# --- Links template ---
_links = {
    "detail": {"operationId": "organizations:detail", "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"}},
    "update": {"operationId": "organizations:update", "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"}},
    "delete": {"operationId": "organizations:delete", "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"}},
    "create_project": {"operationId": "organizations:projects:create", "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"}},
    "list_projects": {"operationId": "organizations:projects:list", "parameters": {"organization_id": "$response.header.Location#regex:/organizations/(.+)/"}},
}

# --- Endpoints ---

@api.post(
    "/",
    name="organizations:create",
    status_code=201,
    response_model=Organization | None,
    responses={
        201: {"headers": {"Location": {"description": "URL of the created item", "schema": {"type": "string"}}}, "links": _links},
        401: Unauthenticated,
        409: Conflict,
    },
)
async def create_organization(
    parameters: OrganizationCreate,
    user: UserDep,
    session: SessionDep,
    request: Request,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    org = Organization(**parameters.model_dump(), users=[user])
    session.add(org)
    try:
        await session.commit()
    except IntegrityError as e:
        if "organization_name_key" in str(e):
            raise HTTPException(409, f"Organization {parameters.name} already exists") from e
        raise
    await session.refresh(org)
    entity_url = url_path_for(request, "organizations:detail", organization_id=org.id)
    return JSONResponse(
        content=org.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )

@api.get(
    "/",
    name="organizations:list",
    response_model=Sequence[Organization],
    responses={401: Unauthenticated},
)
async def list_organizations(user: UserDep):
    """List all organizations the authenticated user belongs to."""
    return await user.awaitable_attrs.organizations

@instance_api.get(
    "/",
    name="organizations:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail_organization(organization: OrganizationDep):
    return organization

@instance_api.put(
    "/",
    name="organizations:update",
    status_code=204,
    responses={
        204: {"content": None, "headers": {"Location": {"description": "URL of the updated item", "schema": {"type": "string"}}}, "links": _links},
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def update_organization(
    parameters: OrganizationUpdate,
    organization: OrganizationDep,
    session: SessionDep,
    request: Request,
):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        setattr(organization, key, value)
    try:
        await session.commit()
    except IntegrityError as e:
        raise HTTPException(409, f"Organization {parameters.name} already exists") from e
    return Response(
        status_code=204,
        headers={"Location": url_path_for(request, "organizations:detail", organization_id=await organization.awaitable_attrs.id)},
    )

@instance_api.delete(
    "/",
    name="organizations:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete_organization(organization: OrganizationDep, session: SessionDep):
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
def list_organization_audits(
    _from: Annotated[datetime, Query(alias="from")],
    _to: Annotated[datetime, Query(alias="to")],
) -> OrganizationAuditLog:
    return OrganizationAuditLog(result=[], retention_period=0)
