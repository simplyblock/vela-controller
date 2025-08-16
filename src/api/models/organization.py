from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from pydantic import BaseModel, StrictBool
from sqlalchemy import BigInteger
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from .._util import Slug
from ..db import SessionDep

if TYPE_CHECKING:
    from .project import Project
    from .user import User


class OrganizationUserLink(AsyncAttrs, SQLModel, table=True):
    organization_id: int | None = Field(default=None, foreign_key='organization.id', primary_key=True)
    user_id: UUID = Field(foreign_key='user.id', primary_key=True)


class Organization(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: Slug = Field(unique=True)
    locked: bool = False
    projects: list['Project'] = Relationship(back_populates='organization', cascade_delete=True)
    users: list['User'] = Relationship(back_populates='organizations', link_model=OrganizationUserLink)


class OrganizationCreate(BaseModel):
    name: Slug
    locked: StrictBool = False


class OrganizationUpdate(BaseModel):
    name: Slug | None = None
    locked: StrictBool | None = None


async def _lookup(session: SessionDep, organization_slug: Slug) -> Organization:
    try:
        return (await session.exec(select(Organization).where(Organization.name == organization_slug))).one()
    except NoResultFound as e:
        raise HTTPException(404, f'Organization {organization_slug} not found') from e


OrganizationDep = Annotated[Organization, Depends(_lookup)]
