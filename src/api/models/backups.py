from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship

from ._util import Model
from ..._util import Identifier

if TYPE_CHECKING:
    from .branch import Branch


class BackupSchedule(AsyncAttrs, Model, table=True):
    __table_args__ = (UniqueConstraint(
        "organization_id",
        "branch_id", "env_type",
        name="unique_backup_schedule",
        postgresql_nulls_not_distinct=True,
    ),)
    organization_id: Identifier | None = Model.foreign_key_field("organization")
    # organization: Organization = Relationship(back_populates="backup_schedules")
    branch_id: Identifier | None = Model.foreign_key_field("branch")
    # branch: Branch = Relationship(back_populates="backup_schedules")
    env_type: str | None = Field(default=None)
    # rows: List["BackupScheduleRow"] = Relationship(back_populates="backup_schedule", cascade_delete=True)


class BackupScheduleRow(AsyncAttrs, Model, table=True):
    schedule_id: Identifier = Model.foreign_key_field("backupschedule")
    # schedule: BackupSchedule = Relationship(back_populates="backup_schedule_rows")
    row_index: int
    interval: int
    unit: str
    retention: int


class NextBackup(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    # branch: Branch = Relationship(back_populates="branches")
    schedule_id: Identifier = Model.foreign_key_field("backupschedule")
    # schedule: BackupSchedule = Relationship(back_populates="next_backups")
    row_index: int
    next_at: datetime


class BackupEntry(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    branch: Branch = Relationship()
    row_index: int
    created_at: datetime
    size_bytes: int


class BackupLog(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    # branch: Branch = Relationship(back_populates="branches")
    backup_uuid: str
    # backup: BackupEntry = Relationship(back_populates="backup_entries")
    action: str
    ts: datetime


class BackupLogCreate(BaseModel):
    backup_uuid: str
    action: str
    ts: datetime


class BackupLogUpdate(BaseModel):
    backup_uuid: str
    action: str
    ts: datetime


class BackupPublic(BaseModel):
    id: Identifier
    organization_id: Identifier
    project_id: Identifier
    branch_id: Identifier
    row_index: int
    created_at: datetime


class BackupScheduleRowPublic(BaseModel):
    row_index: int
    interval: int
    unit: str
    retention: int


class BackupSchedulePublic(BaseModel):
    id: Identifier
    organization_id: Identifier | None
    branch_id: Identifier | None
    env_type: str | None
    rows: list[BackupScheduleRowPublic]


class BackupScheduleCreatePublic(BaseModel):
    status: str
    schedule_id: Identifier


class BackupScheduleDeletePublic(BaseModel):
    status: str
    message: str


class BackupCreatePublic(BaseModel):
    status: str
    backup_id: Identifier


class BackupDeletePublic(BaseModel):
    status: str
    message: str | None


class BackupInfoPublic(BaseModel):
    schedule_id: Identifier
    branch_id: Identifier
    level: str
    next_backup: datetime
