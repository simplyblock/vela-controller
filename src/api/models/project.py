from typing import Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import BigInteger
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel

from ..._util import Int64, Slug
from ...deployment import DeploymentParameters, DeploymentStatus
from ..db import SessionDep
from .organization import Organization


class Project(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    name: Slug
    organization_id: int | None = Field(default=None, foreign_key='organization.id')
    organization: Organization | None = Relationship(back_populates='projects')
    database: str
    database_user: str

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError('Model not tracked in database')
        return self.id

class ProjectCreate(BaseModel):
    name: Slug
    deployment: DeploymentParameters


class ProjectUpdate(BaseModel):
    name: Slug | None = None


class ProjectPublic(BaseModel):
    name: Slug
    status: DeploymentStatus


async def _lookup(session: SessionDep, project_id: Int64) -> Project:
    result = await session.get(Project, project_id)
    if result is None:
        raise HTTPException(404, f'Project {project_id} not found')
    return result


ProjectDep = Annotated[Project, Depends(_lookup)]
