import secrets
from collections.abc import Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlmodel import and_, select

from ..models.membership import Membership
from ..models.role import AccessRight, Role, RoleAccessRight, RoleUserLink, RoleUserLinkPublic, UserPermissionPublic
from ..models.user import User, UserParameters, UserPublic
from ._util import NotFound, Unauthenticated
from .auth import authenticated_user
from .db import SessionDep
from .keycloak import realm_admin

api = APIRouter(dependencies=[Depends(authenticated_user)], tags=["user"])


class UserCreationResult(BaseModel):
    id: UUID
    password: str


async def public(id_: UUID) -> UserPublic:
    user = await realm_admin("vela").a_get_user(str(id_))
    sessions = await realm_admin("vela").a_get_sessions(str(id_))
    return UserPublic(
        id=user["id"],
        email=user["email"],
        first_name=user["firstName"],
        last_name=user["lastName"],
        email_verified=user["emailVerified"],
        active=user["enabled"],
        mfa_enabled=user.get("totp", False),
        last_activity_at=next(iter(sorted([datetime.fromtimestamp(session.lastAccess) for session in sessions])), None),
    )


async def public_list(
    users: Sequence[User],
    response: Literal["shallow", "deep"] = "shallow",
) -> Sequence[UUID | UserPublic]:
    if response == "shallow":
        return [user.id for user in users]
    elif response == "deep":
        return [(await public(user.id)) for user in users]
    else:
        raise AssertionError("unreachable")


@api.post(
    "/",
    status_code=201,
    responses={401: Unauthenticated},
)
async def add(parameters: UserParameters) -> tuple[UserCreationResult, int]:
    password = secrets.token_hex(16)
    user_id = await realm_admin("vela").a_create_user(
        {
            "email": parameters.email,
            "enabled": True,
            "firstName": parameters.first_name,
            "lastName": parameters.last_name,
            "credentials": [
                {
                    "value": password,
                    "type": "password",
                    "temporary": True,
                }
            ],
        }
    )
    await realm_admin("vela").a_send_verify_email(user_id)
    return UserCreationResult(
        id=UUID(user_id),
        password=password,
    ), 201


@api.get(
    "/{user_ref}/",
    responses={401: Unauthenticated, 404: NotFound},
)
async def get(user_ref: UUID | EmailStr) -> UserPublic:
    user_id = (
        UUID(await realm_admin("vela").a_get_user_id(str(user_ref))) if isinstance(user_ref, EmailStr) else user_ref
    )
    return await public(user_id)


@api.get("/{user_ref}/roles/")
async def list_user_roles(
    session: SessionDep,
    user_ref: UUID | EmailStr,
) -> list[RoleUserLinkPublic]:
    user_id = (
        UUID(await realm_admin("vela").a_get_user_id(str(user_ref))) if isinstance(user_ref, EmailStr) else user_ref
    )
    result = await session.execute(select(RoleUserLink).where(RoleUserLink.user_id == user_id))
    return [
        RoleUserLinkPublic(
            organization_id=row.organization_id,
            project_id=row.project_id,
            branch_id=row.branch_id,
            role_id=row.role_id,
            user_id=row.user_id,
            env_type=row.env_type,
        )
        for row in result.scalars().all()
    ]


@api.get("/{user_ref}/permissions/")
async def list_user_permissions(
    session: SessionDep,
    user_ref: UUID | EmailStr,
) -> list[UserPermissionPublic]:
    user_id = (
        UUID(await realm_admin("vela").a_get_user_id(str(user_ref))) if isinstance(user_ref, EmailStr) else user_ref
    )
    stmt = (
        select(  # type: ignore[call-overload]
            AccessRight.entry,
            RoleUserLink.organization_id,
            RoleUserLink.project_id,
            RoleUserLink.branch_id,
            RoleUserLink.env_type,
        )
        .select_from(RoleUserLink)
        .join(Role, Role.id == RoleUserLink.role_id, isouter=True)
        .join(RoleAccessRight, RoleAccessRight.role_id == Role.id, isouter=True)
        .join(AccessRight, AccessRight.id == RoleAccessRight.access_right_id, isouter=True)
        .join(User, User.id == RoleUserLink.user_id, isouter=True)
        .join(
            Membership,
            and_(Membership.user_id == User.id, Membership.organization_id == RoleUserLink.organization_id),
            isouter=True,
        )
        .where(and_(Role.is_active, User.id == user_id))
    )

    result = await session.execute(stmt)

    def is_organization_level_permission(row):
        return row.project_id is None and row.branch_id is None and row.env_type is None

    return [
        UserPermissionPublic(
            permission=row.entry,
            organization_id=row.organization_id if is_organization_level_permission(row) else None,
            project_id=row.project_id,
            branch_id=row.branch_id,
            env_type=row.env_type,
        )
        for row in result.all()
    ]
