from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, StringConstraints
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .project import Project


class Organization(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: Annotated[str, StringConstraints(pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$')]
    projects: list['Project'] = Relationship(back_populates='organization', cascade_delete=True)


class OrganizationCreate(BaseModel):
    name: Annotated[str, StringConstraints(pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$')]
