from collections.abc import Sequence
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlmodel import and_
from sqlmodel import delete as dbdelete

from ..._util import Identifier
from ...models.role import Role, RoleUserLink
from ...models.user import UserPublic
from .._util import Forbidden, NotFound, Unauthenticated, url_path_for
from ..dependencies import OrganizationDep, RoleDep, SessionDep, UserDep, user_lookup
from ..user import public_list as public_user_list

api = APIRouter(tags=["role"])


@api.get(
    "/",
    name="organizations:roles:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_(organization: OrganizationDep) -> Sequence[Role]:
    return await organization.awaitable_attrs.roles


@api.post(
    "/",
    name="organizations:roles:create",
    status_code=201,
    response_model=Role | None,
    responses={
        201: {
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": {
                "detail": {
                    "operationId": "organizations:roles:detail",
                    "parameters": {"role_id": "$response.header.Location#regex:/roles/(.+)/"},
                },
                "update": {
                    "operationId": "organizations:roles:update",
                    "parameters": {"role_id": "$response.header.Location#regex:/roles/(.+)/"},
                },
                "delete": {
                    "operationId": "organizations:roles:delete",
                    "parameters": {"role_id": "$response.header.Location#regex:/roles/(.+)/"},
                },
            },
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
    },
)
async def create(
    session: SessionDep,
    request: Request,
    organization: OrganizationDep,
    user: UserDep,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    entity = Role(organization=organization, user=user)
    session.add(entity)
    await session.commit()
    await session.refresh(entity)
    entity_url = url_path_for(
        request,
        "organizations:roles:detail",
        organization_id=await organization.awaitable_attrs.id,
        role_id=entity.id,
    )
    return JSONResponse(
        content=entity.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{role_id}", tags=["role"])


@instance_api.get(
    "/",
    name="organizations:roles:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(_organization: OrganizationDep, role: RoleDep) -> Role:
    return role


@instance_api.put(
    "/",
    name="organizations:roles:update",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update(_organization: OrganizationDep, _role: RoleDep):
    return Response(content="", status_code=204)


@instance_api.delete(
    "/",
    name="organizations:roles:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(session: SessionDep, _organization: OrganizationDep, role: RoleDep):
    await session.delete(role)
    await session.commit()
    return Response(status_code=204)


@instance_api.get(
    "/users/",
    name="organizations:roles:users:list",
    status_code=200,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_users(
    role: RoleDep,
    response: Literal["shallow", "deep"] = "shallow",
) -> Sequence[UUID | UserPublic]:
    return await public_user_list(await role.awaitable_attrs.users, response)


@instance_api.post(
    "/users/",
    name="organizations:roles:users:add",
    status_code=201,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def add_user(session: SessionDep, role: RoleDep, user_id: UUID) -> Response:
    user = await user_lookup(session, user_id)
    role.users.append(user)
    await session.commit()
    return Response("", status_code=201)


@instance_api.delete(
    "/users/{user_id}/",
    name="organizations:roles:users:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def remove_user(session: SessionDep, role_id: Identifier, user_id: UUID):
    statement = dbdelete(RoleUserLink).where(
        and_(
            RoleUserLink.user_id == user_id,
            RoleUserLink.role_id == role_id,
        )
    )
    result = await session.exec(statement)
    await session.commit()
    if result.rowcount == 0:
        raise HTTPException(404, f"User {user_id} not part of role {role_id}")
    elif result.rowcount == 1:
        return Response("", status_code=204)
    else:
        raise AssertionError("Unreachable")


api.include_router(instance_api)
