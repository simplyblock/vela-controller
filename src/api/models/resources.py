from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field

from ._util import Model
from ..._util import Identifier


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
    org = "org"
    org_env = "org_env"
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
    max_total: int
    max_per_branch: int


class BranchProvisioning(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch", nullable=True)
    resource: ResourceType
    amount: int
    updated_at: datetime


class ProvisioningLog(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch", nullable=True)
    resource: ResourceType
    amount: int
    action: str
    reason: str | None = None
    ts: datetime


class ResourceUsageMinute(AsyncAttrs, Model, table=True):
    ts_minute: datetime
    org_id: Identifier = Model.foreign_key_field("organization", nullable=True)
    project_id: Identifier = Model.foreign_key_field("project", nullable=True)
    branch_id: Identifier = Model.foreign_key_field("branch", nullable=True)
    resource: ResourceType
    amount: int


class ResourceConsumptionLimit(AsyncAttrs, Model, table=True):
    entity_type: EntityType
    org_id: Identifier | None = Model.foreign_key_field("organization", nullable=True)
    project_id: Identifier | None = Model.foreign_key_field("project", nullable=True)
    resource: ResourceType
    max_total_minutes: int
