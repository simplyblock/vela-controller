from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel, StrictBool
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, select

from ..._util import Identifier
from ..db import SessionDep
from ._util import Model, Name
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

class OrganizationCreate(BaseModel):
    name: Name
    locked: StrictBool = False
    require_mfa: StrictBool = False

class OrganizationUpdate(BaseModel):
    name: Name | None = None
    locked: StrictBool | None = None
    require_mfa: StrictBool | None = None

async def _lookup(session: SessionDep, organization_id: Identifier) -> Organization:
    try:
        return (await session.exec(select(Organization).where(Organization.id == organization_id))).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Organization {organization_id} not found") from e


OrganizationDep = Annotated[Organization, Depends(_lookup)]
