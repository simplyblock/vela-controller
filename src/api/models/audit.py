from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr

from ._util import Name


class ActionMetadata(BaseModel):
    method: str | None
    status: str | None


class Action(BaseModel):
    metadata: list[ActionMetadata]
    name: str


class Actor(BaseModel):
    id: str
    type: Literal["user"] | str
    metadata: list[EmailStr | None]


class TargetMetadata(BaseModel):
    organization: Name | None
    project: Name | None


class Target(BaseModel):
    description: str
    metadata: list[TargetMetadata]


class AuditLog(BaseModel):
    action: Action
    actor: Actor
    target: Target
    occurred_at: datetime


class OrganizationAuditLog(BaseModel):
    result: list[AuditLog]
    retention_period: int
