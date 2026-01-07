from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel

from .._util import Identifier
from ._util import Model
from .organization import Organization

if TYPE_CHECKING:
    from .user import User


RoleTypePublic = Literal["organization", "environment", "project", "branch"]


AccessRightPublic = Literal[
    "org:owner:admin",
    "org:settings:read",
    "org:settings:admin",
    "org:auth:read",
    "org:auth:admin",
    "org:backup:read",
    "org:backup:update",
    "org:backup:create",
    "org:backup:delete",
    "org:metering:read",
    "org:role:read",
    "org:role:admin",
    "org:user:read",
    "org:user:admin",
    "org:role-assign:read",
    "org:role-assign:admin",
    "org:projects:read",
    "org:projects:write",
    "org:projects:create",
    "org:projects:stop",
    "org:projects:pause",
    "org:projects:delete",
    "org:projects:pause",
    "org:projects:apikeys",
    "org:limits:read",
    "env:db:admin",
    "env:projects:read",
    "env:projects:admin",
    "org:limits:admin",
    "env:backup:read",
    "env:backup:admin",
    "env:projects:read",
    "env:projects:write",
    "env:projects:create",
    "env:role-assign:read",
    "env:role-assign:admin",
    "env:projects:stop",
    "env:projects:pause",
    "env:projects:delete",
    "env:projects:getkeys",
    "env:db:admin",
    "env:projects:read",
    "env:projects:admin",
    "project:settings:read",
    "project:settings:write",
    "project:role-assign:read",
    "project:role-assign:admin",
    "project:branches:create",
    "project:branches:delete",
    "project:branches:stop",
    "branch:settings:read",
    "branch:settings:admin",
    "branch:role-assign:read",
    "branch:role-assign:admin",
    "branch:auth:read",
    "branch:auth:admin",
    "branch:api:getkeys",
    "branch:replicate:read",
    "branch:replicate:admin",
    "branch:import:read",
    "branch:import:admin",
    "branch:logging:read",
    "branch:monitoring:read",
    "branch:db:admin",
    "branch:rls:read",
    "branch:rls:admin",
    "branch:edge:read",
    "branch:edge:admin",
    "branch:rt:read",
    "branch:rt:admin",
]


class RoleType(PyEnum):
    organization = 0
    environment = 1
    project = 2
    branch = 3


class RoleUserLink(AsyncAttrs, SQLModel, table=True):
    organization_id: Identifier = Model.foreign_key_field("organization", nullable=False, primary_key=True)
    role_id: Identifier = Model.foreign_key_field("role", nullable=False, primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)
    env_types: list[str] = Field(default_factory=list, sa_column=Column(ARRAY(String), nullable=False))
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
    access_rights: list["RoleAccessRight"] = Relationship(back_populates="role", cascade_delete=True)


class RoleAccessRight(AsyncAttrs, Model, table=True):
    organization_id: Identifier = Model.foreign_key_field("organization", nullable=False, primary_key=True)
    role_id: Identifier = Model.foreign_key_field("role", nullable=False, primary_key=True)
    role: Role = Relationship(back_populates="access_rights")
    access_right_id: Identifier = Model.foreign_key_field("accessright", nullable=False, primary_key=True)


class RoleCreate(BaseModel):
    name: str
    role_type: RoleTypePublic
    is_active: bool = True
    is_deletable: bool = True
    description: str | None = None
    access_rights: list[AccessRightPublic] | None = []


class RoleUpdate(BaseModel):
    name: str
    role_type: RoleTypePublic
    is_active: bool = True
    access_rights: list[AccessRightPublic] | None = []
    description: str | None = None


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
    env_types: list[str]


class RoleAssignmentPublic(BaseModel):
    status: str
    count: int
    links: list[RoleUserLinkPublic]


class RoleUnassignmentPublic(BaseModel):
    status: str
    count: int


class RoleWithPermissionsPublic(RolePublic):
    access_rights: list[str] | None


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
    env_types: list[str]
