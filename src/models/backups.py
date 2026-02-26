from datetime import datetime
from typing import Annotated

from pydantic import BaseModel
from sqlalchemy import BigInteger, Column, String, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship

from .._util import Identifier
from ._util import DateTime, Model
from .branch import Branch
from .organization import Organization


class BackupSchedule(AsyncAttrs, Model, table=True):
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "branch_id",
            "env_type",
            name="unique_backup_schedule",
            postgresql_nulls_not_distinct=True,
        ),
    )
    organization_id: Identifier | None = Model.foreign_key_field("organization")
    organization: Organization | None = Relationship()
    branch_id: Identifier | None = Model.foreign_key_field("branch")
    branch: Branch | None = Relationship(back_populates="backup_schedules")
    env_type: str | None = Field(default=None)
    rows: list["BackupScheduleRow"] = Relationship(back_populates="schedule", cascade_delete=True)
    next_backups: list["NextBackup"] = Relationship(back_populates="schedule", cascade_delete=True)


class BackupScheduleRow(AsyncAttrs, Model, table=True):
    schedule_id: Identifier = Model.foreign_key_field("backupschedule")
    schedule: BackupSchedule = Relationship(back_populates="rows")
    row_index: int
    interval: int
    unit: str
    retention: int


class NextBackup(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    branch: Branch = Relationship(back_populates="next_backups")
    schedule_id: Identifier = Model.foreign_key_field("backupschedule")
    schedule: BackupSchedule = Relationship(back_populates="next_backups")
    row_index: int
    next_at: DateTime


class BackupEntry(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    branch: Branch = Relationship(back_populates="backup_entries")
    row_index: int
    created_at: DateTime
    size_bytes: Annotated[int, Field(sa_column=Column(BigInteger, nullable=True))]
    snapshot_uuid: Annotated[str, Field(sa_column=Column(String(length=64), nullable=False))]
    snapshot_name: str | None = Field(
        default=None,
        sa_column=Column(String(length=255), nullable=True),
    )
    snapshot_namespace: str | None = Field(
        default=None,
        sa_column=Column(String(length=255), nullable=True),
    )
    snapshot_content_name: str | None = Field(
        default=None,
        sa_column=Column(String(length=255), nullable=True),
    )
    wal_snapshot_uuid: str | None = Field(
        default=None,
        sa_column=Column(String(length=64), nullable=True),
    )
    wal_snapshot_name: str | None = Field(
        default=None,
        sa_column=Column(String(length=255), nullable=True),
    )
    wal_snapshot_namespace: str | None = Field(
        default=None,
        sa_column=Column(String(length=255), nullable=True),
    )
    wal_snapshot_content_name: str | None = Field(
        default=None,
        sa_column=Column(String(length=255), nullable=True),
    )


class BackupLog(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    branch: Branch = Relationship(back_populates="backup_logs")
    backup_uuid: str
    # backup: BackupEntry = Relationship(back_populates="backup_entries")
    action: str
    ts: DateTime


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
    size_bytes: int
    created_at: datetime


class BackupScheduleRowPublic(BaseModel):
    row_index: int
    interval: int
    unit: str
    retention: int


class BackupSchedulePublic(BaseModel):
    id: Identifier
    organization_id: Identifier | None
    project_id: Identifier | None
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


class BackupInfoPublic(BaseModel):
    schedule_id: Identifier
    branch_id: Identifier
    level: str
    next_backup: datetime
