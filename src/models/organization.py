from typing import TYPE_CHECKING

from pydantic import BaseModel, StrictBool
from sqlmodel import Field, Relationship, SQLModel

from ._util import Slug

if TYPE_CHECKING:
    from .project import Project


class Organization(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: Slug
    locked: bool = False
    projects: list['Project'] = Relationship(back_populates='organization', cascade_delete=True)


class OrganizationCreate(BaseModel):
    name: Slug
    locked: StrictBool = False


class OrganizationUpdate(BaseModel):
    name: Slug | None = None
    locked: StrictBool | None = None
