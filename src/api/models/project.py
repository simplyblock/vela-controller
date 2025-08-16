from typing import Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import BigInteger, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ...deployment import DeploymentParameters, DeploymentStatus
from .._util import Slug
from ..db import SessionDep
from .organization import Organization, OrganizationDep


class Project(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: Slug
    organization_id: int | None = Field(default=None, foreign_key='organization.id')
    organization: Organization | None = Relationship(back_populates='projects')
    database: str
    database_user: str

    __table_args__ = (
        UniqueConstraint("organization_id", "name", name="unique_project_name"),
    )

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError('Model not tracked in database')
        return self.id


class ProjectCreate(BaseModel):
    name: Slug
    deployment: DeploymentParameters


class ProjectUpdate(BaseModel):
    name: Slug | None = None


class ProjectPublic(BaseModel):
    name: Slug
    status: DeploymentStatus


async def _lookup(session: SessionDep, organization: OrganizationDep, project_slug: Slug) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.name == project_slug)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f'Project {project_slug} not found') from e


ProjectDep = Annotated[Project, Depends(_lookup)]
