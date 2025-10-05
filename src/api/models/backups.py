#!/usr/bin/env python3
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from .base import Base  # or wherever your Base is defined
from sqlalchemy.dialects.postgresql import UUID as saUUID
from ..db import *
from datetime import datetime

# --- BACKUPS ---
class BackupSchedule(Base):
    __tablename__ = "backup_schedules"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(saUUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True)
    organization = relationship("Organization")
    env_type = Column(String, nullable=True)
    branch_id = Column(saUUID(as_uuid=True), ForeignKey("branches.id"), nullable=True)
    branch = relationship("Branch")
    rows = relationship(
        "BackupScheduleRow",
        back_populates="schedule",
        cascade="all, delete-orphan",
        single_parent=True
    )

class BackupScheduleRow(Base):
    __tablename__ = "backup_schedule_rows"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schedule_id = Column(saUUID(as_uuid=True), ForeignKey("backup_schedules.id", ondelete="CASCADE"), nullable=False)
    schedule = relationship("BackupSchedule", back_populates="rows")
    row_index = Column(Integer, nullable=False)
    interval = Column(Integer, nullable=False)
    unit = Column(String, nullable=False)
    retention = Column(Integer, nullable=False)
    __table_args__ = (
        Index("ix_backup_schedule_row_schedule_index", "schedule_id", "row_index", unique=True),
    )

class NextBackup(Base):
    __tablename__ = "next_backups"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    branch_id = Column(saUUID(as_uuid=True), ForeignKey("branches.id"), nullable=False)
    branch = relationship("Branch")
    schedule_id = Column(saUUID(as_uuid=True), nullable=False)
    row_index = Column(Integer, nullable=False)
    next_at = Column(DateTime, nullable=False, index=True)
    __table_args__ = (
        Index("ix_next_backups_branch_row", "branch_id", "row_index", unique=True),
    )

class BackupEntry(Base):
    __tablename__ = "backups"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    branch_id = Column(saUUID(as_uuid=True), ForeignKey("branches.id"), nullable=False)
    branch = relationship("Branch")
    row_index = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    size_bytes = Column(Integer, nullable=True)

class BackupLog(Base):
    __tablename__ = "backup_logs"
    id = Column(saUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    branch_id = Column(saUUID(as_uuid=True), nullable=False)
    backup_uuid = Column(saUUID(as_uuid=True), nullable=False)
    action = Column(String, nullable=False)
    ts = Column(DateTime, nullable=False, default=datetime.utcnow)


