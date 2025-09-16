from collections.abc import Sequence

from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse
from sqlmodel import select

from ..._util import Forbidden, NotFound, Unauthenticated
from ...auth import MemberDep, UserDep, authenticated_user
from ...db import SessionDep
from ...models.organization import OrganizationDep
from ...models.user import User, UserPublic, UserRequest

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
async def add_member(
    session: SessionDep,
    organization: OrganizationDep,
    parameters: UserRequest,
):
    # check if user exists
    user_ent = (await session.exec(select(User).where(User.id == parameters.id))).one_or_none()

    if not user_ent:
        user_ent = User(id=parameters.id)
        session.add(user_ent)

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

    return JSONResponse(status_code=201, content=None)


@api.put(
    "/{user_id}",
    name="organizations:members:update",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update_member(user: UserDep):
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
async def remove_member(session: SessionDep, organization: OrganizationDep, user: UserDep, _: MemberDep):
    # Remove user from organization
    org_users = await organization.awaitable_attrs.users
    org_users.remove(user)
    await session.commit()
    return Response(status_code=204)
