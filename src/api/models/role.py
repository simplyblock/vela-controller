from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Annotated, Literal
from uuid import UUID

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ..._util import Identifier
from ..db import SessionDep
from ._util import Model
from .organization import Organization, OrganizationDep

if TYPE_CHECKING:
    from .user import User


RoleTypePublic = Literal["organization", "environment", "project", "branch"]


class RoleType(PyEnum):
    organization = 0
    environment = 1
    project = 2
    branch = 3


class RoleUserLink(AsyncAttrs, SQLModel, table=True):
    organization_id: Identifier = Model.foreign_key_field("organization", nullable=False, primary_key=True)
    role_id: Identifier = Model.foreign_key_field("role", nullable=False, primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)
    env_type: str | None
    project_id: Identifier | None = Model.foreign_key_field("project", nullable=True)
    branch_id: Identifier | None = Model.foreign_key_field("branch", nullable=True)


class AccessRight(AsyncAttrs, Model, table=True):
    entry: str
    role_type: RoleType


class Role(AsyncAttrs, Model, table=True):
    name: str
    organization_id: Identifier = Model.foreign_key_field("organization", nullable=True)
    organization: Organization = Relationship(back_populates="roles")
    users: list["User"] = Relationship(back_populates="roles", link_model=RoleUserLink)
    role_type: RoleType
    is_active: bool
    is_deletable: bool = True
    description: str | None = None
    access_rights: list["RoleAccessRight"] = Relationship(back_populates="role")


class RoleAccessRight(AsyncAttrs, Model, table=True):
    organization_id: Identifier = Model.foreign_key_field("organization", nullable=False, primary_key=True)
    role_id: Identifier = Model.foreign_key_field("role", nullable=False, primary_key=True)
    role: Role = Relationship(back_populates="access_rights")
    access_right_id: Identifier = Model.foreign_key_field("accessright", nullable=False, primary_key=True)


class RolePublic(BaseModel):
    id: Identifier
    organization_id: Identifier
    name: str
    description: str | None
    role_type: RoleTypePublic
    is_active: bool
    is_deletable: bool
    user_count: int


class RoleDeletePublic(BaseModel):
    status: str


class RoleUserLinkPublic(BaseModel):
    organization_id: Identifier
    project_id: Identifier | None
    branch_id: Identifier | None
    role_id: Identifier
    user_id: UUID
    env_type: str | None


class RoleAssignmentPublic(BaseModel):
    status: str
    count: int
    links: list[RoleUserLinkPublic]


class RoleUnassignmentPublic(BaseModel):
    status: str
    count: int


class RoleWithPermissionsPublic(RolePublic):
    access_rights: list[str] | None


class RoleAssignmentsPublic(BaseModel):
    count: int
    links: list[RoleUserLinkPublic]


class PermissionAccessCheckContext(BaseModel):
    organization_id: Identifier
    project_id: Identifier | None
    branch_id: Identifier | None
    env_type: str | None


class PermissionCheckContextPublic(BaseModel):
    organization_id: Identifier | None
    project_id: Identifier | None
    branch_id: Identifier | None
    env_type: str | None


class PermissionAccessCheckPublic(BaseModel):
    access_granted: bool
    context: PermissionCheckContextPublic


class UserPermissionPublic(BaseModel):
    permission: str
    organization_id: Identifier | None
    project_id: Identifier | None
    branch_id: Identifier | None
    env_type: str | None


async def _lookup(session: SessionDep, organization: OrganizationDep, role_id: Identifier) -> Role:
    try:
        query = select(Role).where(Role.id == role_id, Role.organization_id == organization.id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Role {role_id} not found") from e


RoleDep = Annotated[Role, Depends(_lookup)]
