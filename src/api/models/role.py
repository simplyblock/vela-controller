from typing import TYPE_CHECKING, Annotated
from uuid import UUID
from enum import Enum as PyEnum

from fastapi import Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ..._util import Identifier
from ..db import get_db
from sqlmodel.ext.asyncio.session import AsyncSession

SessionDep = Annotated[AsyncSession, Depends(get_db)]
from ._util import Model
from .organization import Organization, OrganizationDep

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .user import User  # forward reference for type hints
    from .role import RoleUserLink

class RoleType(PyEnum):
    organization = 0
    environment = 1
    project = 2
    branch = 3

class RoleAccessRight(AsyncAttrs, Model, table=True):
    role_id: Identifier = Model.foreign_key_field("role", nullable=False, primary_key=True)
    access_right_id: Identifier = Model.foreign_key_field("accessright", nullable=False, primary_key=True)

class RoleUserLink(AsyncAttrs, Model, table=True):
    role_id: int | None = Model.foreign_key_field("role", nullable=True, primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)
    environment_entity: str = Field(nullable=True)
    project_entity: Identifier | None = Model.foreign_key_field("project", nullable=True)
    branch_entity: Identifier | None = Model.foreign_key_field("branch", nullable=True)



class Role(AsyncAttrs, Model, table=True):
    organization_id: Identifier | None = Model.foreign_key_field("organization", nullable=True)
    organization: Organization | None = Relationship(back_populates="roles")
    role_type: RoleType
    is_active: bool


class AccessRight(AsyncAttrs, Model, table=True):
    entry: str
    role_type: RoleType

async def _lookup(session: SessionDep, organization: OrganizationDep, role_id: Identifier) -> Role:
    try:
        query = select(Role).where(Role.id == role_id, Role.organization_id == organization.id)
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Role {role_id} not found") from e


RoleDep = Annotated[Role, Depends(_lookup)]
