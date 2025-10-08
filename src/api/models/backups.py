from __future__ import annotations
from datetime import datetime
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import SQLModel, Field, Relationship
from ._util import Model
from ..._util import Identifier
from .branch import Branch
from .organization import Organization, OrganizationDep

class BackupSchedule(AsyncAttrs, Model, table=True):
    organization_id: Identifier = Model.foreign_key_field("organization")
    #organization: Organization = Relationship(back_populates="backup_schedules")
    branch_id: Identifier = Model.foreign_key_field("branch")
    #branch: Branch = Relationship(back_populates="backup_schedules")
    env_type: str
    #rows: List["BackupScheduleRow"] = Relationship(back_populates="backup_schedule", cascade_delete=True)


class BackupScheduleRow(AsyncAttrs, Model, table=True):
    schedule_id: Identifier = Model.foreign_key_field("backupschedule")
    #schedule: BackupSchedule = Relationship(back_populates="backup_schedule_rows")
    row_index: int
    interval: int
    unit: str
    retention: int

class NextBackup(AsyncAttrs, Model,  table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    #branch: Branch = Relationship(back_populates="branches")
    schedule_id: Identifier = Model.foreign_key_field("backupschedule")
    #schedule: BackupSchedule = Relationship(back_populates="next_backups")
    row_index: int
    next_at: datetime


class BackupEntry(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    #branch: Branch = Relationship(back_populates="branches")
    row_index: int
    created_at: datetime
    size_bytes: int

class BackupLog(AsyncAttrs, Model,  table=True):
    branch_id: Identifier = Model.foreign_key_field("branch")
    #branch: Branch = Relationship(back_populates="branches")
    backup_uuid: Identifier = Model.foreign_key_field("backupentry")
    #backup: BackupEntry = Relationship(back_populates="backup_entries")
    action: str
    ts: datetime