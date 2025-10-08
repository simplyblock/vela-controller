from enum import Enum as PyEnum
from datetime import datetime
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncAttrs
from ._util import Model
from ..._util import Identifier

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
class ResourceLimit(AsyncAttrs, Model,  table=True):
    entity_type: EntityType
    resource: ResourceType
    org_id: Identifier = Model.foreign_key_field("organization")
    env_type: Optional[str] = None
    project_id: Identifier = Model.foreign_key_field("project")
    max_total: int
    max_per_branch: int


class BranchProvisioning(AsyncAttrs, Model,  table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    resource: ResourceType
    amount: int
    updated_at: datetime


class ProvisioningLog(AsyncAttrs, Model,  table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    resource: ResourceType
    amount: int
    action: str
    reason: Optional[str] = None
    ts: datetime


class ResourceUsageMinute(AsyncAttrs, Model,  table=True):
    ts_minute: datetime
    org_id: Identifier = Model.foreign_key_field("organization")
    project_id: Identifier = Model.foreign_key_field("project")
    branch_id: Identifier = Model.foreign_key_field("branch")
    resource: ResourceType
    amount: int


class ResourceConsumptionLimit(AsyncAttrs, Model,  table=True):
    entity_type: EntityType
    org_id: Identifier = Model.foreign_key_field("organization")
    project_id: Identifier = Model.foreign_key_field("project")
    resource: ResourceType
    max_total_minutes: int
