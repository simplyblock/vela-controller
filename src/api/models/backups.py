from datetime import datetime

from typing import TYPE_CHECKING
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Relationship, Field

from ..._util import Identifier
from ._util import Model
from .organization import Organization

if TYPE_CHECKING:
    from .branch import Branch

# --- BACKUPS ---
class BackupSchedule(AsyncAttrs, Model, table=True):
    organization_id: Identifier = Model.foreign_key_field("organization")
    organization: "Organization" = Relationship(back_populates="schedules")
    branch_id: Identifier = Model.foreign_key_field("branch")
    branch: "Branch" = Relationship(back_populates="schedules")
    rows: list["BackupScheduleRow"] = Relationship(back_populates="schedule", cascade_delete=True)
    env_type : str

class BackupScheduleRow(AsyncAttrs, Model, table=True):
    schedule_id: Identifier = Field(foreign_key="backupschedules.id", nullable=False)
    schedule: "BackupSchedule" = Relationship(back_populates="rows")

    row_index: int
    interval: int
    unit: str
    retention: int


class NextBackup(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Field(foreign_key="branches.id", nullable=False)
    branch: "Branch" = Relationship(back_populates="next_backups")

    schedule_id: Identifier
    row_index: int
    next_at: datetime


class BackupEntry(AsyncAttrs, Model, table=True):
    branch_id: Identifier = Field(foreign_key="branches.id", nullable=False)
    branch: "Branch" = Relationship(back_populates="backup_entries")

    row_index: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    size_bytes: int


class BackupLog(AsyncAttrs, Model, table=True):
    branch_id: Identifier
    backup_uuid: Identifier
    action: str
    ts: datetime = Field(default_factory=datetime.utcnow)


