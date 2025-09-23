from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel, StrictBool
from sqlalchemy import BigInteger, event
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ..db import SessionDep
from ._util import Name, Slug, update_slug
from .membership import Membership

if TYPE_CHECKING:
    from .project import Project
    from .role import Role
    from .user import User


class Organization(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    slug: Slug = Field(unique=True)
    name: Name
    locked: bool = False
    projects: list["Project"] = Relationship(back_populates="organization", cascade_delete=True)
    roles: list["Role"] = Relationship(back_populates="organization", cascade_delete=True)
    users: list["User"] = Relationship(back_populates="organizations", link_model=Membership)
    require_mfa: bool = False


event.listen(Organization, "before_insert", update_slug)
event.listen(Organization, "before_update", update_slug)


class OrganizationCreate(BaseModel):
    name: Name
    locked: StrictBool = False
    require_mfa: StrictBool = False


class OrganizationUpdate(BaseModel):
    name: Name | None = None
    locked: StrictBool | None = None
    require_mfa: StrictBool | None = None


async def _lookup(session: SessionDep, organization_slug: Slug) -> Organization:
    try:
        return (await session.exec(select(Organization).where(Organization.slug == organization_slug))).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Organization {organization_slug} not found") from e


OrganizationDep = Annotated[Organization, Depends(_lookup)]
