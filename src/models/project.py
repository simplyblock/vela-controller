from typing import TYPE_CHECKING, Literal, Optional

from pydantic import BaseModel, model_validator
from sqlalchemy import Column, String, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship

from .._util import (
    DB_SIZE_MIN,
    IOPS_MIN,
    MEMORY_MIN,
    STORAGE_SIZE_MIN,
    VCPU_MILLIS_MIN,
    Identifier,
    Name,
)
from ._util import Model
from .organization import Organization
from .resources import ResourceLimitsPublic

if TYPE_CHECKING:
    from .branch import Branch
    from .resources import ResourceLimit


ProjectStatus = Literal[
    "PAUSING",
    "PAUSED",
    "STARTING",
    "STARTED",
    "MIGRATING",
    "DELETING",
    "ERROR",
    "UNKNOWN",
]


class Project(AsyncAttrs, Model, table=True):
    name: Name
    max_backups: int
    status: ProjectStatus = Field(
        default="STARTED",
        sa_column=Column(String(length=32), nullable=False, server_default="STARTED"),
    )
    organization_id: Identifier = Model.foreign_key_field("organization")
    organization: Organization = Relationship(back_populates="projects")
    branches: list["Branch"] = Relationship(back_populates="project", cascade_delete=True)
    resource_limit: Optional["ResourceLimit"] = Relationship(back_populates="project", cascade_delete=True)

    __table_args__ = (UniqueConstraint("organization_id", "name", name="unique_project_name"),)


class ProjectCreate(BaseModel):
    name: Name
    per_branch_limits: ResourceLimitsPublic
    project_limits: ResourceLimitsPublic
    max_backups: int

    @model_validator(mode="after")
    def _validate_limits(self):
        minimums: dict[str, int] = {
            "milli_vcpu": VCPU_MILLIS_MIN,
            "ram": MEMORY_MIN,
            "iops": IOPS_MIN,
            "storage_size": STORAGE_SIZE_MIN,
            "database_size": DB_SIZE_MIN,
        }
        for resource_name in ResourceLimitsPublic.model_fields:
            per_branch_value = getattr(self.per_branch_limits, resource_name)
            project_value = getattr(self.project_limits, resource_name)
            min_value = minimums.get(resource_name)
            if project_value is not None and min_value is not None and project_value < min_value:
                raise ValueError(
                    f"project_limits.{resource_name} ({project_value}) is below the minimum allowed value ({min_value})"
                )
            if per_branch_value is None:
                continue
            if project_value is None:
                raise ValueError(
                    f"per_branch_limits.{resource_name} is set but project_limits.{resource_name} is not defined"
                )
            if min_value is not None and per_branch_value < min_value:
                raise ValueError(
                    f"per_branch_limits.{resource_name} ({per_branch_value}) is below "
                    f"the minimum allowed value ({min_value})"
                )
            if per_branch_value > project_value:
                raise ValueError(
                    f"per_branch_limits.{resource_name} ({per_branch_value}) exceeds "
                    f"project_limits.{resource_name} ({project_value})"
                )
        return self


class ProjectUpdate(BaseModel):
    name: Name | None = None
    max_backups: int | None = None


class ProjectPublic(BaseModel):
    organization_id: Identifier
    id: Identifier
    name: Name
    max_backups: int
    status: ProjectStatus
