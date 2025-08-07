from pydantic import BaseModel
from sqlalchemy import BigInteger
from sqlmodel import Field, Relationship, SQLModel

from ._util import Slug
from .organization import Organization


class Project(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: Slug
    organization_id: int | None = Field(default=None, foreign_key='organization.id')
    organization: Organization | None = Relationship(back_populates='projects')


class ProjectCreate(BaseModel):
    name: Slug


class ProjectUpdate(BaseModel):
    name: Slug | None = None
