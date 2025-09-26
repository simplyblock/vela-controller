from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select
from ulid import ULID

from ...deployment import DeploymentParameters
from .._util import ULIDType
from ..db import DBULID, SessionDep
from ._util import Name
from .organization import Organization, OrganizationDep

if TYPE_CHECKING:
    from .branch import Branch


class Project(AsyncAttrs, SQLModel, table=True):
    id: ULID = Field(default_factory=ULID, primary_key=True, sa_type=DBULID)
    name: Name
    organization_id: ULID = Field(foreign_key="organization.id", sa_type=DBULID)
    organization: Organization = Relationship(back_populates="projects")
    database: str
    database_user: str
    database_password: str
    branches: list["Branch"] = Relationship(back_populates="project", cascade_delete=True)


class ProjectCreate(BaseModel):
    name: Name
    deployment: DeploymentParameters


class ProjectUpdate(BaseModel):
    name: Name | None = None


class ProjectPublic(BaseModel):
    organization_id: ULIDType
    id: ULIDType
    name: Name
    status: str
    deployment_status: tuple[str, dict[str, str]]
    database_user: str
    encrypted_database_connection_string: str


async def _lookup(session: SessionDep, organization: OrganizationDep, project_id: ULIDType) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.id == project_id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_id} not found") from e


ProjectDep = Annotated[Project, Depends(_lookup)]
