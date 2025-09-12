from typing import Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import BigInteger, UniqueConstraint, event
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ...deployment import DeploymentParameters
from ..db import SessionDep
from ._util import Name, Slug, update_slug
from .organization import Organization, OrganizationDep


class Project(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    slug: Slug
    name: Name
    organization_id: int | None = Field(default=None, foreign_key="organization.id")
    organization: Organization | None = Relationship(back_populates="projects")
    database: str
    database_user: str

    __table_args__ = (UniqueConstraint("organization_id", "slug", name="unique_project_slug"),)

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError("Model not tracked in database")
        return self.id

    def db_org_id(self) -> int:
        if self.organization_id is None:
            raise ValueError("Model not tracked in database")
        return self.organization_id


event.listens_for(Project, "before_insert", update_slug)
event.listens_for(Project, "before_update", update_slug)


class ProjectCreate(BaseModel):
    name: Name
    deployment: DeploymentParameters


class ProjectUpdate(BaseModel):
    name: Name | None = None


class ProjectPublic(BaseModel):
    organization_id: int
    id: int
    name: Name
    status: str
    deployment_status: tuple[str, dict[str, str]]


async def _lookup(session: SessionDep, organization: OrganizationDep, project_slug: Slug) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.slug == project_slug)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_slug} not found") from e


ProjectDep = Annotated[Project, Depends(_lookup)]
