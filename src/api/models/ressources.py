from ..db import *
from datetime import datetime
import uuid
from datetime import datetime
from sqlalchemy import String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from enum import Enum as PyEnum
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import Column, Integer
from .base import Base
from sqlalchemy.dialects.postgresql import UUID as saUUID

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

# --- RESOURCE LIMITS & PROVISIONING ---
class ResourceLimit(Base):
    __tablename__ = "resource_limits"
    id = Column(Integer, primary_key=True)
    entity_type = Column(SQLEnum(EntityType), nullable=False)
    resource = Column(SQLEnum(ResourceType), nullable=False)
    org_id = Column(saUUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    env_type = Column(String, nullable=True)
    project_id = Column(saUUID(as_uuid=True), ForeignKey("projects.id"), nullable=True)
    max_total = Column(BigInteger, nullable=False)
    max_per_branch = Column(BigInteger, nullable=False)
    __table_args__ = (
        UniqueConstraint("entity_type", "org_id", "env_type", "project_id", "resource"),
    )

class BranchProvisioning(Base):
    __tablename__ = "branch_provisionings"
    id = Column(Integer, primary_key=True)
    branch_id = Column(saUUID(as_uuid=True), ForeignKey("branches.id"), nullable=False)
    resource = Column(Enum(ResourceType), nullable=False)
    amount = Column(BigInteger, nullable=False)
    updated_at = Column(TIMESTAMP, default=datetime.utcnow)
    __table_args__ = (
        UniqueConstraint("branch_id", "resource"),
    )

class ProvisioningLog(Base):
    __tablename__ = "provisioning_log"
    id = Column(Integer, primary_key=True)
    branch_id = Column(saUUID(as_uuid=True), ForeignKey("branches.id"))
    resource = Column(Enum(ResourceType), nullable=False)
    amount = Column(BigInteger, nullable=False)
    action = Column(String, nullable=False)
    reason = Column(String, nullable=True)
    ts = Column(TIMESTAMP, default=datetime.utcnow)

class ResourceUsageMinute(Base):
    __tablename__ = "resource_usage_minutes"
    id = Column(Integer, primary_key=True)
    ts_minute = Column(TIMESTAMP, nullable=False)
    org_id = Column(saUUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    project_id = Column(saUUID(as_uuid=True), ForeignKey("projects.id"), nullable=False)
    branch_id = Column(saUUID(as_uuid=True), ForeignKey("branches.id"), nullable=False)
    resource = Column(Enum(ResourceType), nullable=False)
    amount = Column(BigInteger, nullable=False)
    __table_args__ = (
        UniqueConstraint("ts_minute", "org_id", "project_id", "branch_id", "resource"),
    )

class ResourceConsumptionLimit(Base):
    __tablename__ = "resource_consumption_limits"
    id = Column(Integer, primary_key=True)
    entity_type = Column(Enum(EntityType), nullable=False)
    org_id = Column(saUUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    project_id = Column(saUUID(as_uuid=True), ForeignKey("projects.id"), nullable=True)
    resource = Column(Enum(ResourceType), nullable=False)
    max_total_minutes = Column(BigInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("entity_type", "org_id", "project_id", "resource"),
    )

