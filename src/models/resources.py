from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField
from sqlalchemy import BigInteger, Index, text
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship
from ulid import ULID

from .._util import Identifier
from ._util import DatabaseIdentifier, DateTime, Model

if TYPE_CHECKING:
    from .organization import Organization
    from .project import Project


# ---------------------------
# Enums
# ---------------------------
class ResourceType(PyEnum):
    milli_vcpu = "milli_vcpu"
    ram = "ram"
    iops = "iops"
    storage_size = "storage_size"
    database_size = "database_size"


class EntityType(PyEnum):
    system = "system"
    org = "org"
    project = "project"


# ---------------------------
# RESOURCE LIMITS & PROVISIONING
# ---------------------------
class ResourceLimit(AsyncAttrs, Model, table=True):
    # Ensure unqiueness across resource type for global-, org-, or project-level limits
    __table_args__ = (
        Index(
            "uq_limit_global",
            "entity_type",
            "resource",
            unique=True,
            postgresql_where=text("organization_id IS NULL AND project_id IS NULL"),
        ),
        Index(
            "uq_limit_org",
            "entity_type",
            "resource",
            "organization_id",
            unique=True,
            postgresql_where=text("project_id IS NULL"),
        ),
        Index(
            "uq_limit_env",
            "entity_type",
            "resource",
            "organization_id",
            "env_type",
            unique=True,
            postgresql_where=text("organization_id IS NOT NULL AND env_type IS NOT NULL"),
        ),
        Index(
            "uq_limit_project",
            "entity_type",
            "resource",
            "organization_id",
            "project_id",
            unique=True,
            postgresql_where=text("organization_id IS NOT NULL AND project_id IS NOT NULL"),
        ),
    )

    entity_type: EntityType
    resource: ResourceType
    organization_id: Identifier | None = Model.foreign_key_field("organization", ondelete="CASCADE")
    organization: Optional["Organization"] = Relationship(back_populates="limits")
    env_type: str | None = None
    project_id: Identifier | None = Model.foreign_key_field("project", ondelete="CASCADE")
    project: Optional["Project"] = Relationship(back_populates="limits")
    max_total: Annotated[int, Field(sa_type=BigInteger)]
    max_per_branch: Annotated[int, Field(sa_type=BigInteger)]


class BranchProvisioning(AsyncAttrs, Model, table=True):
    branch_id: Identifier | None = Model.foreign_key_field("branch", ondelete="CASCADE")
    resource: ResourceType
    amount: Annotated[int, Field(sa_type=BigInteger)]
    updated_at: DateTime


class ProvisioningLog(AsyncAttrs, Model, table=True):
    branch_id: Identifier | None = Model.foreign_key_field("branch", ondelete="CASCADE")
    resource: ResourceType
    amount: Annotated[int, Field(sa_type=BigInteger)]
    action: str
    reason: str | None = None
    ts: DateTime


class ResourceUsageMinute(AsyncAttrs, Model, table=True):
    ts_minute: DateTime
    organization_id: Identifier | None = Model.foreign_key_field("organization", ondelete="CASCADE")
    project_id: Identifier | None = Model.foreign_key_field("project", ondelete="SET NULL")
    original_project_id: Identifier = Field(
        sa_type=DatabaseIdentifier
    )  # Required to allow discrimination after the origin project has been deleted
    branch_id: Identifier | None = Model.foreign_key_field("branch", ondelete="SET NULL")
    original_branch_id: Identifier = Field(
        sa_type=DatabaseIdentifier
    )  # Required to allow discrimination after the origin branch has been deleted
    resource: ResourceType
    amount: Annotated[int, Field(sa_type=BigInteger)]


class ResourceConsumptionLimit(AsyncAttrs, Model, table=True):
    entity_type: EntityType
    organization_id: Identifier | None = Model.foreign_key_field("organization", ondelete="CASCADE")
    project_id: Identifier | None = Model.foreign_key_field("project", ondelete="CASCADE")
    resource: ResourceType
    max_total_minutes: int


ResourceTypePublic = Literal["milli_vcpu", "ram", "iops", "storage_size", "database_size"]


class ResourceLimitsPublic(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "milli_vcpu": None,
                "ram": None,
                "iops": None,
                "storage_size": None,
                "database_size": None,
            }
        }
    )

    milli_vcpu: int | None = PydanticField(
        default=None,
        description="Requested milli vCPU per branch; omit or null to inherit higher-level limit.",
    )
    ram: int | None = PydanticField(
        default=None,
        description="Requested RAM (bytes) per branch; omit or null to inherit higher-level limit.",
    )
    iops: int | None = PydanticField(
        default=None,
        description="Requested IOPS per branch; omit or null to inherit higher-level limit.",
    )
    storage_size: int | None = PydanticField(
        default=None,
        description="Requested storage size (bytes) per branch; omit or null to inherit higher-level limit.",
    )
    database_size: int | None = PydanticField(
        default=None,
        description="Requested database size (bytes) per branch; omit or null to inherit higher-level limit.",
    )


class ProvLimitPayload(BaseModel):
    resource: ResourceTypePublic
    max_total: int
    max_per_branch: int


class ConsumptionPayload(BaseModel):
    resource: ResourceTypePublic
    max_total_minutes: int


class BranchProvisionPublic(BaseModel):
    status: str


class BranchAllocationPublic(BaseModel):
    branch_id: Identifier
    milli_vcpu: int | None = None
    ram: int | None = None
    iops: int | None = None
    storage_size: int | None = None
    database_size: int | None = None


class LimitResultPublic(BaseModel):
    status: str
    limit: ULID


class ProvisioningLimitPublic(BaseModel):
    resource: ResourceTypePublic
    max_total: int
    max_per_branch: int

    @classmethod
    def from_limit(cls, limit):
        return cls(
            resource=limit.resource.value,
            max_total=limit.max_total,
            max_per_branch=limit.max_per_branch,
        )


class ConsumptionLimitPublic(BaseModel):
    resource: ResourceTypePublic
    max_total_minutes: int


class UsageCycle(BaseModel):
    start: datetime | None
    end: datetime | None


class ResourceLimitDefinitionPublic(BaseModel):
    resource_type: ResourceTypePublic
    min: int
    max: int
    step: int
    unit: str | None
