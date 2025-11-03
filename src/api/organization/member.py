from collections.abc import Sequence
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from ...models.user import UserID, UserPublic
from .._util import Forbidden, NotFound, Unauthenticated
from ..auth import authenticated_user, user_by_id
from ..dependencies import MemberDep, OrganizationDep, SessionDep, UserDep
from ..user import public_list as public_user_list

api = APIRouter(dependencies=[Depends(authenticated_user)])


@api.get(
    "/",
    name="organizations:members:list",
    status_code=200,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_users(
    organization: OrganizationDep,
    response: Literal["shallow", "deep"] = "shallow",
) -> Sequence[UUID | UserPublic]:
    return await public_user_list(await organization.awaitable_attrs.users, response)


@api.post(
    "/",
    name="organizations:members:add",
    status_code=201,
    responses={
        201: {
            "content": None,
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": {
                "update": {
                    "operationId": "organizations:members:update",
                    "parameters": {"user_id": "$request.body#/id"},
                },
                "delete": {
                    "operationId": "organizations:members:remove",
                    "parameters": {"user_id": "$request.body#/id"},
                },
            },
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
    },
)
async def add(
    session: SessionDep,
    organization: OrganizationDep,
    parameters: UserID,
):
    user = await user_by_id(session, parameters.id)

    # add user to organization
    (await organization.awaitable_attrs.users).append(user)
    try:
        await session.commit()
    except IntegrityError as e:
        error = str(e)
        if ("asyncpg.exceptions.UniqueViolationError" not in error) or ("unique_membership" not in error):
            raise
        raise HTTPException(400, f"User {parameters.id} is already member of organization {organization.id}") from e

    return JSONResponse(status_code=201, content=None)


@api.put(
    "/{user_id}",
    name="organizations:members:update",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update(_: UserDep):
    # no op
    return JSONResponse(
        status_code=204,
        content=None,
    )


@api.delete(
    "/{user_id}",
    name="organizations:members:remove",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def remove(session: SessionDep, organization: OrganizationDep, user: UserDep, _: MemberDep):
    # Remove user from organization
    org_users = await organization.awaitable_attrs.users
    org_users.remove(user)
    await session.commit()
    return Response(status_code=204)
