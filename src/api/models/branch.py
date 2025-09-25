from typing import Annotated, ClassVar, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import BigInteger, Column, String, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel, select

from ..._util import Slug
from ..db import SessionDep
from ._util import Name
from .project import Project, ProjectDep


class Branch(AsyncAttrs, SQLModel, table=True):
    DEFAULT_SLUG: ClassVar[Slug] = "main"

    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: Slug
    project_id: int | None = Field(default=None, foreign_key="project.id")
    project: Project | None = Relationship(back_populates="branches")
    parent_id: int | None = Field(default=None, foreign_key="branch.id")
    parent: Optional["Branch"] = Relationship(sa_relationship_kwargs={"remote_side": "Branch.id"})
    endpoint_domain: str | None = Field(default=None, sa_column=Column(String(255), nullable=True))

    # Deployment parameters specific to this branch
    database_size: Annotated[int, Field(gt=0, multiple_of=2**30, sa_column=Column(BigInteger))]
    vcpu: Annotated[int, Field(gt=0, le=2**31 - 1, sa_column=Column(BigInteger))]
    memory: Annotated[int, Field(gt=0, multiple_of=2**30, sa_column=Column(BigInteger))]
    iops: Annotated[int, Field(gt=0, le=2**31 - 1, sa_column=Column(BigInteger))]
    database_image_tag: str

    __table_args__ = (UniqueConstraint("project_id", "name", name="unique_branch_name_per_project"),)

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError("Model not tracked in database")
        return self.id

    def db_project_id(self) -> int:
        if self.project_id is None:
            raise ValueError("Project model not tracked in database")
        return self.project_id


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
    name: Slug


class BranchDetailResources(BaseModel):
    vcpu: int
    ram_mb: int
    nvme_gb: int
    iops: int
    storage_gb: int


async def _lookup(session: SessionDep, project: ProjectDep, branch: Slug) -> Branch:
    try:
        query = select(Branch).where(Branch.project_id == project.id, Branch.name == branch)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Branch {branch} not found") from e


BranchDep = Annotated[Branch, Depends(_lookup)]
