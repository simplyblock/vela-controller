from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional
from enum import Enum as PyEnum
from uuid import UUID

# ---------------------------
# Enums
# ---------------------------
class ResourceType(PyEnum):
    vcpu = "vcpu"
    ram = "ram"
    iops = "iops"
    backup_storage = "backup_storage"
    nvme = "nvme"

class EntityType(PyEnum):
    org = "org"
    org_env = "org_env"
    project = "project"


# ---------------------------
# RESOURCE LIMITS & PROVISIONING
# ---------------------------
class ResourceLimit(SQLModel, table=True):
    id: Optional[UUID] = Field(default=None, primary_key=True)
    entity_type: EntityType
    resource: ResourceType
    org_id: UUID = Field(foreign_key="organizations.id")
    env_type: Optional[str] = None
    project_id: Optional[UUID] = Field(default=None, foreign_key="projects.id")
    max_total: int
    max_per_branch: int


class BranchProvisioning(SQLModel, table=True):
    id: Optional[UUID] = Field(default=None, primary_key=True)
    branch_id: UUID = Field(foreign_key="branches.id")
    resource: ResourceType
    amount: int
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ProvisioningLog(SQLModel, table=True):
    id: Optional[UUID] = Field(default=None, primary_key=True)
    branch_id: Optional[UUID] = Field(default=None, foreign_key="branches.id")
    resource: ResourceType
    amount: int
    action: str
    reason: Optional[str] = None
    ts: datetime = Field(default_factory=datetime.utcnow)


class ResourceUsageMinute(SQLModel, table=True):
    id: Optional[UUID] = Field(default=None, primary_key=True)
    ts_minute: datetime
    org_id: UUID = Field(foreign_key="organizations.id")
    project_id: UUID = Field(foreign_key="projects.id")
    branch_id: UUID = Field(foreign_key="branches.id")
    resource: ResourceType
    amount: int


class ResourceConsumptionLimit(SQLModel, table=True):
    id: Optional[UUID] = Field(default=None, primary_key=True)
    entity_type: EntityType
    org_id: UUID = Field(foreign_key="organizations.id")
    project_id: Optional[UUID] = Field(default=None, foreign_key="projects.id")
    resource: ResourceType
    max_total_minutes: int
