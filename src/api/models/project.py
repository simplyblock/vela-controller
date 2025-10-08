from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Relationship, select

from ..._util import Identifier
from ...deployment import DeploymentParameters
from ..db import SessionDep
from ._util import Model, Name
from .organization import Organization, OrganizationDep

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
    max_backups: int
    deployment: DeploymentParameters

class ProjectUpdate(BaseModel):
    name: Name | None = None
    max_backups: int | None = None


class ProjectPublic(BaseModel):
    organization_id: Identifier
    id: Identifier
    name: Name
    max_backups: int
    branch_status: dict[Any, StatusType]


async def _lookup(session: SessionDep, organization: OrganizationDep, project_id: Identifier) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.id == project_id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_id} not found") from e


ProjectDep = Annotated[Project, Depends(_lookup)]
