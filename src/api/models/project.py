from typing import TYPE_CHECKING, Annotated, Literal

from fastapi import Depends, HTTPException
from pydantic import BaseModel, model_validator
from sqlalchemy import Column, String, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, select

from ..._util import Identifier
from ..db import SessionDep
from ._util import Model, Name
from .organization import Organization, OrganizationDep
from .resources import ResourceLimitsPublic

if TYPE_CHECKING:
    from .branch import Branch


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

    __table_args__ = (UniqueConstraint("organization_id", "name", name="unique_project_name"),)


class ProjectCreate(BaseModel):
    name: Name
    per_branch_limits: ResourceLimitsPublic
    project_limits: ResourceLimitsPublic
    max_backups: int

    @model_validator(mode="after")
    def _validate_limits(self):
        for resource_name in ResourceLimitsPublic.model_fields:
            per_branch_value = getattr(self.per_branch_limits, resource_name)
            project_value = getattr(self.project_limits, resource_name)
            if per_branch_value is None:
                continue
            if project_value is None:
                raise ValueError(
                    f"per_branch_limits.{resource_name} is set but project_limits.{resource_name} is not defined"
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
    default_branch_id: Identifier | None  # TODO @Manohar please fill in the correct value


async def _lookup(session: SessionDep, organization: OrganizationDep, project_id: Identifier) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.id == project_id)
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_id} not found") from e


ProjectDep = Annotated[Project, Depends(_lookup)]
