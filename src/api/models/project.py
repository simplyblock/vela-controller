from typing import TYPE_CHECKING, Annotated, Literal

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Relationship, select

from ..._util import Identifier
from ..db import SessionDep
from ._util import Model, Name
from .organization import Organization, OrganizationDep
from .resources import ResourceLimitsPublic

if TYPE_CHECKING:
    from .branch import Branch


class Project(AsyncAttrs, Model, table=True):
    name: Name
    max_backups: int
    organization_id: Identifier = Model.foreign_key_field("organization")
    organization: Organization = Relationship(back_populates="projects")
    branches: list["Branch"] = Relationship(back_populates="project", cascade_delete=True)

    __table_args__ = (UniqueConstraint("organization_id", "name", name="unique_project_name"),)


class ProjectCreate(BaseModel):
    name: Name
    per_branch_limits: ResourceLimitsPublic
    project_limits: ResourceLimitsPublic
    max_backups: int


class ProjectUpdate(BaseModel):
    name: Name | None = None
    max_backups: int | None = None


ProjectStatus = Literal[
    "PAUSING",
    "PAUSED",
    "STARTING",
    "STARTED",
    "MIGRATING",
    "DELETING",
    "ERROR",
    "UNKNOWN",
]


class ProjectPublic(BaseModel):
    organization_id: Identifier
    id: Identifier
    name: Name
    max_backups: int
    status: ProjectStatus  # TODO @Manohar please fill in the correct status
    default_branch_id: Identifier | None  # TODO @Manohar please fill in the correct value


async def _lookup(session: SessionDep, organization: OrganizationDep, project_id: Identifier) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.id == project_id)
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_id} not found") from e


ProjectDep = Annotated[Project, Depends(_lookup)]
