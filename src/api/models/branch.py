from datetime import datetime
from typing import Annotated, ClassVar, Literal, Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from pydantic import Field as PydanticField
from sqlalchemy import BigInteger, Column, String, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, select

from ..._util import GIB, KIB, Identifier, Slug
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
    database_size: Annotated[int, Field(gt=0, multiple_of=GIB, sa_column=Column(BigInteger))]
    vcpu: Annotated[int, Field(gt=0, le=2**31 - 1, sa_column=Column(BigInteger))]
    memory: Annotated[int, Field(gt=0, multiple_of=GIB, sa_column=Column(BigInteger))]
    iops: Annotated[int, Field(ge=100, le=2**31 - 1, sa_column=Column(BigInteger))]
    database_image_tag: str

    __table_args__ = (UniqueConstraint("project_id", "name", name="unique_branch_name_per_project"),)

    def provisioned_resources(self) -> "ResourcesDefinition":
        """Return the resource definition for the branch provisioning envelope."""

        return ResourcesDefinition(
            vcpu=self.vcpu,
            ram_bytes=self.memory,
            nvme_bytes=self.database_size,
            iops=self.iops,
            storage_bytes=self.database_size,
        )


class BranchCreate(BaseModel):
    name: Name
    source: Identifier
    # Clone options (reserved for future use)
    config_copy: bool = False
    data_copy: bool = False


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
            ge=KIB,
            multiple_of=KIB,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            ge=GIB,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            ge=100,
            le=2**31 - 1,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int | None,
        PydanticField(
            ge=GIB,
            description="Database storage capacity in bytes (mirrors Branch.database_size).",
        ),
    ] = None


class ResourceUsageDefinition(BaseModel):
    vcpu: Annotated[
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
    anon: str
    service_role: str


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
            ge=KIB,
            multiple_of=KIB,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            ge=GIB,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            ge=100,
            le=2**31 - 1,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int,
        PydanticField(
            ge=GIB,
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
