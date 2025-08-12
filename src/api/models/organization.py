from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from pydantic import BaseModel, StrictBool
from sqlalchemy import BigInteger
from sqlmodel import Field, Relationship, SQLModel

from ..._util import Int64, Slug
from ..db import SessionDep

if TYPE_CHECKING:
    from .project import Project
    from .user import User


class OrganizationUserLink(SQLModel, table=True):
    organization_id: int | None = Field(default=None, foreign_key='organization.id', primary_key=True)
    user_id: UUID = Field(foreign_key='user.id', primary_key=True)


class Organization(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: Slug
    locked: bool = False
    projects: list['Project'] = Relationship(back_populates='organization', cascade_delete=True)
    users: list['User'] = Relationship(
            back_populates='organizations',
            link_model=OrganizationUserLink,
            sa_relationship_kwargs={'lazy': 'selectin'},
    )


class OrganizationCreate(BaseModel):
    name: Slug
    locked: StrictBool = False


class OrganizationUpdate(BaseModel):
    name: Slug | None = None
    locked: StrictBool | None = None


async def _lookup(session: SessionDep, organization_id: Int64) -> Organization:
    result = await session.get(Organization, organization_id)
    if result is None:
        raise HTTPException(404, f'Organization {organization_id} not found')
    return result


OrganizationDep = Annotated[Organization, Depends(_lookup)]
