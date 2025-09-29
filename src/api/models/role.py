from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ..db import SessionDep
from ._util import Identifier, Model
from .organization import Organization, OrganizationDep

if TYPE_CHECKING:
    from .user import User


class RoleUserLink(AsyncAttrs, SQLModel, table=True):
    role_id: int | None = Model.foreign_key_field("role", nullable=True, primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)


class Role(AsyncAttrs, Model, table=True):
    organization_id: int | None = Model.foreign_key_field("organization", nullable=True)
    organization: Organization | None = Relationship(back_populates="roles")
    users: list["User"] = Relationship(back_populates="roles", link_model=RoleUserLink)


async def _lookup(session: SessionDep, organization: OrganizationDep, role_id: Identifier) -> Role:
    try:
        query = select(Role).where(Role.id == role_id, Role.organization_id == organization.id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Role {role_id} not found") from e


RoleDep = Annotated[Role, Depends(_lookup)]
