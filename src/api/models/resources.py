from datetime import datetime
from enum import Enum as PyEnum
from typing import Annotated, Literal

from pydantic import BaseModel
from sqlalchemy import BigInteger, Column, DateTime
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field
from ulid import ULID

from ..._util import Identifier
from ._util import Model


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
    entity_type: EntityType
    resource: ResourceType
    org_id: Identifier | None = Model.foreign_key_field("organization", nullable=True)
    env_type: str | None = Field(default=None, nullable=True)
    project_id: Identifier | None = Model.foreign_key_field("project", nullable=True)
    max_total: Annotated[int, Field(sa_type=BigInteger)]
    max_per_branch: Annotated[int, Field(sa_type=BigInteger)]


class BranchProvisioning(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch", nullable=True)
    resource: ResourceType
    amount: Annotated[int, Field(sa_type=BigInteger)]
    updated_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))


class ProvisioningLog(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch", nullable=True)
    resource: ResourceType
    amount: Annotated[int, Field(sa_type=BigInteger)]
    action: str
    reason: str | None = None
    ts: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))


class ResourceUsageMinute(AsyncAttrs, Model, table=True):
    ts_minute: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    org_id: Identifier = Model.foreign_key_field("organization", nullable=True)
    project_id: Identifier = Model.foreign_key_field("project", nullable=True)
    branch_id: Identifier = Model.foreign_key_field("branch", nullable=True)
    resource: ResourceType
    amount: Annotated[int, Field(sa_type=BigInteger)]


class ResourceConsumptionLimit(AsyncAttrs, Model, table=True):
    entity_type: EntityType
    org_id: Identifier | None = Model.foreign_key_field("organization", nullable=True)
    project_id: Identifier | None = Model.foreign_key_field("project", nullable=True)
    resource: ResourceType
    max_total_minutes: int


ResourceTypePublic = Literal["milli_vcpu", "ram", "iops", "storage_size", "database_size"]


class ResourceLimitsPublic(BaseModel):
    milli_vcpu: int | None = None
    ram: int | None = None
    iops: int | None = None
    storage_size: int | None = None
    database_size: int | None = None


class ResourcesPayload(BaseModel):
    resources: ResourceLimitsPublic


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
