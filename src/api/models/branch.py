from typing import Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import BigInteger, UniqueConstraint, event
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ._util import Name, Slug, update_slug
from ..db import SessionDep
from .project import Project, ProjectDep


class Branch(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    slug: Slug
    name: Name
    project_id: int | None = Field(default=None, foreign_key="project.id")
    project: Project | None = Relationship(back_populates="branches")
    parent_id: int | None = Field(default=None, foreign_key="branch.id")
    parent: "Branch | None" = Relationship(sa_relationship_kwargs={"remote_side": "Branch.id"})

    __table_args__ = (UniqueConstraint("project_id", "slug", name="unique_branch_slug_per_project"),)


event.listen(Branch, "before_insert", update_slug)
event.listen(Branch, "before_update", update_slug)


class BranchCreate(BaseModel):
    name: Name
    # If provided, the new branch will be cloned from this branch's slug
    source: Slug | None = None
    # Clone options
    config_copy: bool = False
    data_copy: bool = False


class BranchUpdate(BaseModel):
    name: Name | None = None


class BranchPublic(BaseModel):
    id: int
    slug: Slug
    name: Name
    parent_slug: Slug | None = None


async def _lookup(
    session: SessionDep, project: ProjectDep, branch_slug: Slug
) -> Branch:
    try:
        query = select(Branch).where(Branch.project_id == project.id, Branch.slug == branch_slug)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Branch {branch_slug} not found") from e


BranchDep = Annotated[Branch, Depends(_lookup)]

