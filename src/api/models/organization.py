from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel, StrictBool
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, select

from ..._util import Identifier, Name
from ..db import SessionDep
from ._util import Model
from .membership import Membership

if TYPE_CHECKING:
    from .project import Project
    from .role import Role
    from .user import User


class Organization(AsyncAttrs, Model, table=True):
    name: Name = Field(unique=True)
    locked: bool = False
    projects: list["Project"] = Relationship(back_populates="organization", cascade_delete=True)
    roles: list["Role"] = Relationship(back_populates="organization", cascade_delete=True)
    users: list["User"] = Relationship(back_populates="organizations", link_model=Membership)
    require_mfa: bool = False
    max_backups: int
    environments: str


class OrganizationCreate(BaseModel):
    name: Name
    locked: StrictBool = False
    require_mfa: StrictBool = False
    max_backups: int
    environments: str


class OrganizationUpdate(BaseModel):
    name: Name | None = None
    locked: StrictBool | None = None
    require_mfa: StrictBool | None = None
    max_backups: int | None = None
    environments: str | None = None


async def _lookup(session: SessionDep, organization_id: Identifier) -> Organization:
    try:
        return (await session.execute(select(Organization).where(Organization.id == organization_id))).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Organization {organization_id} not found") from e


OrganizationDep = Annotated[Organization, Depends(_lookup)]
