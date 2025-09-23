from typing import Annotated, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import BigInteger, UniqueConstraint, event
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ..db import SessionDep
from ._util import Name, Slug, update_slug
from .project import Project, ProjectDep


class Branch(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    slug: Slug
    name: Name
    project_id: int | None = Field(default=None, foreign_key="project.id")
    project: Project | None = Relationship(back_populates="branches")
    parent_id: int | None = Field(default=None, foreign_key="branch.id")
    parent: Optional["Branch"] = Relationship(sa_relationship_kwargs={"remote_side": "Branch.id"})

    # Deployment parameters specific to this branch
    database_size: int
    vcpu: int
    memory: int
    iops: int
    database_image_tag: str

    __table_args__ = (UniqueConstraint("project_id", "slug", name="unique_branch_slug_per_project"),)

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError("Model not tracked in database")
        return self.id

    def db_project_id(self) -> int:
        if self.project_id is None:
            raise ValueError("Project model not tracked in database")
        return self.project_id


event.listen(Branch, "before_insert", update_slug)
event.listen(Branch, "before_update", update_slug)


class BranchCreate(BaseModel):
    name: Name
    # If provided, the new branch will be cloned from this branch's slug
    source: Slug | None = None
    # Clone options (reserved for future use)
    config_copy: bool = False
    data_copy: bool = False


class BranchUpdate(BaseModel):
    name: Name | None = None


class BranchPublic(BaseModel):
    id: int
    slug: Slug
    name: Name
    parent_slug: Slug | None = None


async def _lookup(session: SessionDep, project: ProjectDep, branch_slug: Slug) -> Branch:
    try:
        query = select(Branch).where(Branch.project_id == project.id, Branch.slug == branch_slug)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Branch {branch_slug} not found") from e


BranchDep = Annotated[Branch, Depends(_lookup)]
