from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ...deployment import delete_deployment
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..auth import UserDep, authenticated_user
from ..db import SessionDep
from ..models.audit import OrganizationAuditLog
from ..models.organization import Organization, OrganizationCreate, OrganizationDep, OrganizationUpdate
from ..models.user import User, UserPublic, UserRequest
from .project import api as project_api

api = APIRouter(dependencies=[Depends(authenticated_user)])


@api.get(
    "/",
    name="organizations:list",
    responses={401: Unauthenticated},
)
async def list_(user: UserDep) -> Sequence[Organization]:
    return await user.awaitable_attrs.organizations


_links = {
    "detail": {
        "operationId": "organizations:detail",
        "parameters": {"organization_slug": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "update": {
        "operationId": "organizations:update",
        "parameters": {"organization_slug": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "delete": {
        "operationId": "organizations:delete",
        "parameters": {"organization_slug": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "create_project": {
        "operationId": "organizations:projects:create",
        "parameters": {"organization_slug": "$response.header.Location#regex:/organizations/(.+)/"},
    },
    "list_projects": {
        "operationId": "organizations:projects:list",
        "parameters": {"organization_slug": "$response.header.Location#regex:/organizations/(.+)/"},
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
    user: UserDep,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    entity = Organization(**parameters.model_dump(), users=[user])
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as e:
        raise HTTPException(409, f"Organization {parameters.name} already exists") from e
    await session.refresh(entity)
    entity_url = url_path_for(request, "organizations:detail", organization_slug=entity.slug)
    return JSONResponse(
        content=entity.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


async def _check_user_access(user: UserDep, organization: OrganizationDep):
    if organization.require_mfa and not user.token.mfa():
        raise HTTPException(401, detail="This operation requires multi-factor authentication")

    if user not in await organization.awaitable_attrs.users:
        raise HTTPException(403, detail="Unauthorized access")


instance_api = APIRouter(
    prefix="/{organization_slug}",
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
                organization_slug=await organization.awaitable_attrs.slug,
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
        delete_deployment(project.dbid())

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


@instance_api.get(
    "/members",
    name="organizations:members:list",
    status_code=200,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_users(organization: OrganizationDep) -> Sequence[UserPublic]:
    return await organization.awaitable_attrs.users


@instance_api.post(
    "/members",
    name="organizations:members:add",
    status_code=201,
    responses={
        201: {"description": "Member added successfully"},
        400: {"description": "User is already a member of this organization"},
        401: Unauthenticated,
        403: Forbidden,
        404: {"description": "User not found"},
    },
)
async def add_member(
    session: SessionDep,
    organization: OrganizationDep,
    parameters: UserRequest,
):
    # check if user exists
    user_ent = (await session.exec(select(User).where(User.name == parameters.name))).one_or_none()

    if not user_ent:
        user_ent = User(id=uuid4(), name=parameters.name)
        session.add(user_ent)
        await session.commit()
        await session.refresh(user_ent)  # ensure fields are loaded

    # check if user already in org
    org_users = await organization.awaitable_attrs.users
    if any(u.id == user_ent.id for u in org_users):
        return JSONResponse(
            status_code=400,
            content={"message": "User is already a member of this organization"},
        )

    # add user to organization
    org_users.append(user_ent)
    await session.commit()
    await session.refresh(user_ent)  # refresh after commit again

    return JSONResponse(
        status_code=201,
        content={
            "message": "Member added successfully",
            "user": {
                "id": str(user_ent.id),
                "name": user_ent.name,
            },
        },
    )


class MemberUpdateRequest(BaseModel):
    name: str | None = None


@instance_api.put(
    "/members/{user_id}",
    name="organizations:members:update",
    responses={
        200: {"description": "Member updated successfully"},
        400: {"description": "Invalid update operation"},
        401: Unauthenticated,
        403: Forbidden,
        404: {"description": "User or member not found"},
    },
)
async def update_member(
    session: SessionDep,
    organization: OrganizationDep,
    user_id: UUID,
    member_data: MemberUpdateRequest,
):
    # Check if target user exists
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Check if user is a member of this organization
    org_users = await organization.awaitable_attrs.users
    if user not in org_users:
        raise HTTPException(status_code=404, detail="User is not a member of this organization")

    updated = False

    # Update name if provided
    if member_data.name:
        user.name = member_data.name
        updated = True

    if not updated:
        raise HTTPException(status_code=400, detail="No valid fields provided for update")

    session.add(user)
    await session.commit()
    await session.refresh(user)

    return {
        "message": "Member updated successfully",
        "user": {
            "id": str(user.id),
            "name": user.name,
        },
    }


@instance_api.delete(
    "/members/{user_id}",
    name="organizations:members:remove",
    status_code=204,
    responses={
        204: {"description": "Member removed successfully"},
        400: {"description": "Cannot remove last admin"},
        401: Unauthenticated,
        403: Forbidden,
        404: {"description": "User or member not found"},
    },
)
async def remove_member(
    session: SessionDep,
    organization: OrganizationDep,
    current_user: UserDep,
    user_id: UUID,
):
    # Prevent removing yourself
    if str(user_id) == str(current_user.id):
        raise HTTPException(status_code=400, detail="Cannot remove yourself. Please ask another admin to remove you.")

    # Check if target user exists
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Check if user is a member
    org_users = await organization.awaitable_attrs.users
    if user not in org_users:
        raise HTTPException(status_code=404, detail="User is not a member of this organization")

    # Remove user from organization
    org_users.remove(user)

    await session.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


instance_api.include_router(project_api, prefix="/projects")

api.include_router(instance_api)
