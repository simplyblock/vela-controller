from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import UniqueConstraint, event
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Relationship, select

from ..._util import Slug
from ...deployment import DeploymentParameters
from ..db import SessionDep
from ._util import Model, Name, update_slug
from .organization import Organization, OrganizationDep

if TYPE_CHECKING:
    from .branch import Branch


class Project(AsyncAttrs, Model, table=True):
    slug: Slug
    name: Name
    organization_id: int | None = Model.foreign_key_field("organization", nullable=True)
    organization: Organization | None = Relationship(back_populates="projects")
    database: str
    database_user: str
    database_password: str
    branches: list["Branch"] = Relationship(back_populates="project", cascade_delete=True)

    __table_args__ = (UniqueConstraint("organization_id", "slug", name="unique_project_slug"),)

    def db_org_id(self) -> int:
        if self.organization_id is None:
            raise ValueError("Organization model not tracked in database")
        return self.organization_id


event.listen(Project, "before_insert", update_slug)
event.listen(Project, "before_update", update_slug)


class ProjectCreate(BaseModel):
    name: Name
    deployment: DeploymentParameters


class ProjectUpdate(BaseModel):
    name: Name | None = None


class ProjectPublic(BaseModel):
    organization_id: int
    id: int
    slug: Slug
    name: Name
    status: str
    deployment_status: tuple[str, dict[str, str]]
    database_user: str
    encrypted_database_connection_string: str


async def _lookup(session: SessionDep, organization: OrganizationDep, project_slug: Slug) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.slug == project_slug)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_slug} not found") from e


ProjectDep = Annotated[Project, Depends(_lookup)]
