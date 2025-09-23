from collections.abc import Sequence

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from .._util import Forbidden, NotFound, Unauthenticated
from ..auth import MemberDep, UserDep, authenticated_user
from ..db import SessionDep
from ..models.organization import OrganizationDep
from ..models.user import User, UserPublic, UserRequest

api = APIRouter(dependencies=[Depends(authenticated_user)])


@api.get(
    "/",
    name="organizations:members:list",
    status_code=200,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_users(organization: OrganizationDep) -> Sequence[UserPublic]:
    return await organization.awaitable_attrs.users


@api.post(
    "/",
    name="organizations:members:add",
    status_code=201,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def add(
    session: SessionDep,
    organization: OrganizationDep,
    parameters: UserRequest,
):
    # check if user exists
    user_ent = (await session.exec(select(User).where(User.id == parameters.id))).one_or_none()

    if not user_ent:
        user_ent = User(id=parameters.id)
        session.add(user_ent)

    # add user to organization
    (await organization.awaitable_attrs.users).append(user_ent)
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
