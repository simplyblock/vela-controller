from typing import Annotated

from pydantic import BaseModel, StringConstraints
from sqlmodel import Field, Relationship, SQLModel

from .organization import Organization


class Project(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: Annotated[str, StringConstraints(pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$')]
    organization_id: int | None = Field(default=None, foreign_key='organization.id')
    organization: Organization | None = Relationship(back_populates='projects')


class ProjectCreate(BaseModel):
    name: Annotated[str, StringConstraints(pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$')]
