from pydantic import BaseModel
from sqlalchemy import BigInteger, UniqueConstraint, event
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel
from typing import Annotated


from ._util import Name, Slug, update_slug
from .organization import Organization
from .project import Project
from ..db import SessionDep
from ._util import Name, Slug, update_slug
from .organization import Organization, OrganizationDep
from sqlmodel import Field, Relationship, SQLModel, select
from fastapi import Depends, HTTPException
from sqlalchemy.exc import NoResultFound

class Branch(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    slug: Slug
    name: Name
    organization_id: int | None = Field(default=None, foreign_key="organization.id")
    organization: Organization | None = Relationship(back_populates="projects")
    project: Project | None = Relationship(back_populates="branches")
    project_id: int | None = Field(default=None, foreign_key="project.id")
    copy_config: bool = Field(default=False)
    copy_data: bool = Field(default=False)

    __table_args__ = (UniqueConstraint("project_id", "slug", name="unique_project_slug"),)

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError("Model not tracked in database")
        return self.id

    def db_org_id(self) -> int:
        if self.organization_id is None:
            raise ValueError("Model not tracked in database")
        return self.organization_id


event.listen(Branch, "before_insert", update_slug)
event.listen(Branch, "before_update", update_slug)


class BranchParameters(BaseModel):
    copy_config: bool = False
    copy_data: bool = False

class BranchCreate(BaseModel):
    name: Name
    params: BranchParameters


class BranchPublic(BaseModel):
    organization_id: int
    id: int
    slug: Slug
    name: Name
    status: str
    deployment_status: tuple[str, dict[str, str]]
    database_user: str
    encrypted_database_connection_string: str
    copy_data: bool
    copy_config: bool


async def _lookup(session: SessionDep, organization: OrganizationDep, branch_slug: Slug) -> Branch:
    try:
        query = select(Branch).where(Branch.organization_id == organization.id, Branch.slug == branch_slug)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Branch {branch_slug} not found") from e


BranchDep = Annotated[Branch, Depends(_lookup)]
