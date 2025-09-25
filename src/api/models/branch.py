from typing import Annotated, ClassVar, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from pydantic import Field as PydanticField
from sqlalchemy import BigInteger, Column, String, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, select

from ..._util import Identifier, Slug
from ..db import SessionDep
from ._util import Model, Name
from .project import Project, ProjectDep


class Branch(AsyncAttrs, Model, table=True):
    DEFAULT_SLUG: ClassVar[Slug] = "main"

    name: Slug
    project_id: Identifier = Model.foreign_key_field("project")
    project: Project | None = Relationship(back_populates="branches")
    parent_id: Identifier | None = Model.foreign_key_field("branch", nullable=True)
    parent: Optional["Branch"] = Relationship()
    endpoint_domain: str | None = Field(default=None, sa_column=Column(String(255), nullable=True))

    # Deployment parameters specific to this branch
    database_size: Annotated[int, Field(gt=0, multiple_of=2**30, sa_column=Column(BigInteger))]
    vcpu: Annotated[int, Field(gt=0, le=2**31 - 1, sa_column=Column(BigInteger))]
    memory: Annotated[int, Field(gt=0, multiple_of=2**30, sa_column=Column(BigInteger))]
    iops: Annotated[int, Field(gt=0, le=2**31 - 1, sa_column=Column(BigInteger))]
    database_image_tag: str

    __table_args__ = (UniqueConstraint("project_id", "name", name="unique_branch_name_per_project"),)


class BranchCreate(BaseModel):
    name: Name
    source: Identifier
    # Clone options (reserved for future use)
    config_copy: bool = False
    data_copy: bool = False


class BranchUpdate(BaseModel):
    name: Name | None = None


class BranchPublic(BaseModel):
    id: Identifier
    name: Slug


class BranchDetailResources(BaseModel):
    vcpu: Annotated[
        int,
        PydanticField(
            ge=1,
            le=2**31 - 1,
            description="Number of virtual CPUs provisioned (matches Branch.vcpu constraints).",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            ge=1024,
            multiple_of=1024,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            ge=2**30,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            ge=1,
            le=2**31 - 1,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int,
        PydanticField(
            ge=2**30,
            description="Database storage capacity in bytes (mirrors Branch.database_size).",
        ),
    ]


async def lookup(session: SessionDep, project: ProjectDep, branch_id: Identifier) -> Branch:
    try:
        query = select(Branch).where(Branch.project_id == project.id, Branch.id == branch_id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Branch {branch_id} not found") from e


BranchDep = Annotated[Branch, Depends(lookup)]
