from datetime import datetime
from typing import Annotated, ClassVar, Literal, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel, model_validator
from pydantic import Field as PydanticField
from sqlalchemy import BigInteger, Column, String, Text, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, select

from ..._util import (
    CPU_CONSTRAINTS,
    DATABASE_SIZE_CONSTRAINTS,
    IOPS_CONSTRAINTS,
    MEMORY_CONSTRAINTS,
    STORAGE_SIZE_CONSTRAINTS,
    Identifier,
    Slug,
)
from ...deployment import DeploymentParameters
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
    database: Annotated[str, Field(sa_column=Column(String(255)))]
    database_user: Annotated[str, Field(sa_column=Column(String(255)))]
    database_password: Annotated[str, Field(sa_column=Column(String(255)))]
    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS, sa_column=Column(BigInteger))]
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS, sa_column=Column(BigInteger))]  # units of milli vCPU
    memory: Annotated[int, Field(**MEMORY_CONSTRAINTS, sa_column=Column(BigInteger))]
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS, sa_column=Column(BigInteger))]
    storage_size: Annotated[int, Field(**STORAGE_SIZE_CONSTRAINTS, sa_column=Column(BigInteger))]
    database_image_tag: str
    jwt_secret: Annotated[str, Field(default=None, sa_column=Column(Text, nullable=True))]
    anon_key: Annotated[str, Field(default=None, sa_column=Column(Text, nullable=True))]
    service_key: Annotated[str, Field(default=None, sa_column=Column(Text, nullable=True))]

    __table_args__ = (UniqueConstraint("project_id", "name", name="unique_branch_name_per_project"),)

    def provisioned_resources(self) -> "ResourcesDefinition":
        """Return the resource definition for the branch provisioning envelope."""

        return ResourcesDefinition(
            milli_vcpu=self.milli_vcpu,
            ram_bytes=self.memory,
            nvme_bytes=self.database_size,
            iops=self.iops,
            storage_bytes=self.storage_size,
        )


class BranchSourceParameters(BaseModel):
    branch_id: Identifier
    config_copy: bool = False
    data_copy: bool = False


class BranchCreate(BaseModel):
    name: Name
    source: BranchSourceParameters | None = None
    deployment: DeploymentParameters | None = None

    @model_validator(mode="after")
    def _validate_source_or_deployment(self) -> "BranchCreate":
        provided = sum(value is not None for value in (self.source, self.deployment))
        if provided != 1:
            raise ValueError("Provide exactly one of source or deployment")
        return self


class BranchUpdate(BaseModel):
    name: Name | None = None


BranchServiceStatus = Literal[
    "ACTIVE_HEALTHY",
    "STOPPED",
    "STARTING",
    "ACTIVE_UNHEALTHY",
    "CREATING",
    "DELETING",
    "UPDATING",
    "RESTARTING",
    "STOPPING",
    "UNKNOWN",
]


class DatabaseInformation(BaseModel):
    host: str
    port: int
    username: str
    name: str
    encrypted_connection_string: str
    service_endpoint_uri: str
    version: str
    has_replicas: bool


class ResourcesDefinition(BaseModel):
    milli_vcpu: Annotated[
        int,
        PydanticField(
            **CPU_CONSTRAINTS,
            description="Number of milli vCPUs provisioned (matches Branch.milli_vcpu constraints).",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            **MEMORY_CONSTRAINTS,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            **DATABASE_SIZE_CONSTRAINTS,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            **IOPS_CONSTRAINTS,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int | None,
        PydanticField(
            **STORAGE_SIZE_CONSTRAINTS,
            description="Storage capacity in bytes to be used for Storage API (mirrors Branch.storage_size).",
        ),
    ] = None


class ResourceUsageDefinition(BaseModel):
    milli_vcpu: Annotated[
        int,
        PydanticField(
            ge=0,
            le=2**31 - 1,
            description="Measured vCPU consumption for the branch.",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            ge=0,
            description="Measured RAM usage in bytes.",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            ge=0,
            description="Measured NVMe usage in bytes.",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            ge=0,
            le=2**31 - 1,
            description="Measured IOPS consumption.",
        ),
    ]
    storage_bytes: Annotated[
        int | None,
        PydanticField(
            ge=0,
            description="Measured storage usage in bytes, if available.",
        ),
    ] = None


class BranchApiKeys(BaseModel):
    anon: str | None
    service_role: str | None


class ApiKeyDetails(BaseModel):
    name: str
    api_key: str
    id: str
    type: Literal["legacy"]
    hash: str
    prefix: str
    description: str


class BranchStatus(BaseModel):
    database: BranchServiceStatus
    storage: BranchServiceStatus
    realtime: BranchServiceStatus
    meta: BranchServiceStatus
    rest: BranchServiceStatus


class BranchPublic(BaseModel):
    id: Identifier
    name: Slug
    project_id: Identifier
    organization_id: Identifier
    database: DatabaseInformation
    max_resources: ResourcesDefinition
    assigned_labels: list[str]
    used_resources: ResourceUsageDefinition
    api_keys: BranchApiKeys
    service_health: BranchStatus
    status: str  # represents the VM status like "Running", "Stopped" etc
    ptir_enabled: bool
    created_at: datetime
    created_by: str
    updated_at: datetime | None = None
    updated_by: str | None = None


class BranchDetailResources(BaseModel):
    milli_vcpu: Annotated[
        int,
        PydanticField(
            **CPU_CONSTRAINTS,
            description="Number of milli vCPUs provisioned (matches Branch.milli_vcpu constraints).",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            **MEMORY_CONSTRAINTS,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            **DATABASE_SIZE_CONSTRAINTS,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            **IOPS_CONSTRAINTS,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int,
        PydanticField(
            **STORAGE_SIZE_CONSTRAINTS,
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
