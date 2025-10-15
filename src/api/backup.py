import logging
import os
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, logger, Request, HTTPException
from pydantic import BaseModel, validator
from sqlmodel import select
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_db
from .models._util import Identifier
from .models.backups import (
    BackupSchedule,
    BackupSchedulePublic,
    BackupScheduleCreatePublic,
    BackupScheduleDeletePublic,
    BackupScheduleRow,
    BackupScheduleRowPublic,
    BackupEntry,
    BackupPublic,
    BackupCreatePublic,
    BackupDeletePublic,
    BackupLog,
    NextBackup, BackupInfoPublic,
)
from .models.branch import Branch
from .models.organization import Organization
from .models.project import Project

router = APIRouter()

# ---------------------------
# Constants
# ---------------------------
UNIT_MULTIPLIER = {
    "min": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
    "w": 604800, "week": 604800, "weeks": 604800,
}

VALID_UNITS = set(UNIT_MULTIPLIER.keys())

INTERVAL_LIMITS = {
    "minute": 59, "minutes": 59, "min": 59,
    "hour": 23, "hours": 23, "h": 23,
    "day": 6, "d": 6, "days": 6,
    "week": 12, "w": 12, "weeks": 12,
}

# ---------------------------
# Pydantic Schemas
# ---------------------------
class ScheduleRow(BaseModel):
    row_index: int
    interval: int
    unit: str
    retention: int

    @validator("unit")
    def unit_must_be_valid(cls, v: str):
        if v not in VALID_UNITS:
            raise ValueError("Invalid unit")
        return v


class SchedulePayload(BaseModel):
    rows: List[ScheduleRow]
    env_type: Optional[str] = None

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("backup-monitor")

# ---------------------------
# Create/Update Schedule
# ---------------------------
@router.post("/backup/organizations/{org_ref}/schedule")
@router.put("/backup/organizations/{org_ref}/schedule")
@router.post("/backup/branches/{branch_ref}/schedule")
@router.put("/backup/branches/{branch_ref}/schedule")
async def add_or_replace_backup_schedule(
    payload: SchedulePayload,
    org_ref: Optional[Identifier] = None,
    branch_ref: Optional[Identifier] = None,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
) -> BackupScheduleCreatePublic:

    if not payload.rows:
        raise HTTPException(status_code=400, detail="No rows provided")
    if len(payload.rows) > 10:
        raise HTTPException(status_code=422, detail="Too many rows in schedule. Max: 10")

    # Resolve organization or branch
    org = branch = project = None
    org_id = branch_id = None

    if org_ref:
        result = await db.execute(select(Organization).where(Organization.id == org_ref))
        org = result.scalars().first()
        if org:
            org_id = org.id
            logger.info("org-level backup:",str(org_id))
    elif branch_ref:
        result = await db.execute(select(Branch).where(Branch.id == branch_ref))
        branch = result.scalars().first()
        if branch:
            branch_id = branch.id
            result = await db.execute(select(Project).where(Project.id == branch.project_id))
            project = result.scalars().first()
            logger.info("branch-level backup:",str(branch_id))

    if not org and not branch:
        raise HTTPException(status_code=404, detail="Valid branch or organization required.")

   # Find existing schedule and eager-load rows
    schedule=None
    env_type=payload.env_type
    if request.method=="PUT":
      if env_type:
        stmt = (
          select(BackupSchedule)
          .where(
            BackupSchedule.organization_id == org_id,
            BackupSchedule.branch_id == branch_id,
            BackupSchedule.env_type == payload.env_type
          )
      )
      else:
        stmt = (
            select(BackupSchedule)
            .where(
                BackupSchedule.organization_id == org_id,
                BackupSchedule.branch_id == branch_id
            )
        )
      result = await db.execute(stmt)
      schedule = result.scalars().first()

    # Validate schedule rows
    total_retention = 0
    seen_keys = set()
    for r in payload.rows:
        key = (r.interval, r.unit)
        if key in seen_keys:
            raise HTTPException(status_code=422, detail="Duplicate row found in schedule")
        seen_keys.add(key)
        if r.interval > INTERVAL_LIMITS.get(r.unit, 9999):
            raise HTTPException(
                status_code=400,
                detail=f"Interval for {r.unit} cannot exceed {INTERVAL_LIMITS.get(r.unit)}",
            )
        total_retention += r.retention

    # Max backups validation
    max_allowed = org.max_backups if org else getattr(project, "max_backups", 0)
    if total_retention > max_allowed:
        entity_type = "Organization" if org else "Branch"
        entity_ref = org_ref if org else branch_ref
        raise HTTPException(
            status_code=422,
            detail=f"Max Backups {max_allowed} of {entity_type} {entity_ref} exceeded: {total_retention}",
        )

    # Delete old rows if schedule exists
    if schedule:
        await db.execute(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))
        await db.execute(delete(NextBackup).where(NextBackup.schedule_id == schedule.id))
        await db.commit()
    else:
        if branch_ref:
           await db.execute(delete(NextBackup).where(NextBackup.branch_id == branch_ref))
        elif env_type is not None:
            stmt = delete(NextBackup).where(
                NextBackup.branch_id.in_(
                    select(Branch.id).where(Branch.env_type == env_type)
                )
            )
            await db.execute(stmt)
            await db.commit()
        schedule = BackupSchedule(
            organization_id=org_id,
            branch_id=branch_id,
            env_type=payload.env_type,
        )
        db.add(schedule)
        await db.commit()
        await db.refresh(schedule)

    # Insert new rows
    for r in payload.rows:
        row = BackupScheduleRow(
            schedule_id=schedule.id,
            row_index=r.row_index,
            interval=r.interval,
            unit=r.unit,
            retention=r.retention,
        )
        db.add(row)

    await db.commit()
    await db.refresh(schedule)
    return BackupScheduleCreatePublic(status="ok", schedule_id=str(schedule.id))

# ---------------------------
# List Schedules
# ---------------------------
@router.get("/backup/organizations/{org_ref}/schedule")
@router.get("/backup/branches/{branch_ref}/schedule")
async def list_schedules(
    org_ref: Optional[Identifier] = None,
    branch_ref: Optional[Identifier] = None,
    env_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
) -> list[BackupSchedulePublic]:
    stmt = select(BackupSchedule)
    if org_ref:
        stmt = stmt.where(BackupSchedule.organization_id == org_ref)
    if branch_ref:
        stmt = stmt.where(BackupSchedule.branch_id == branch_ref)
    if env_type is not None:
        stmt = stmt.where(BackupSchedule.env_type == env_type)

    result = await db.execute(stmt)
    schedules = result.scalars().all()
    if not schedules:
        raise HTTPException(status_code=404, detail="No schedules found.")

    out: list[BackupSchedulePublic] = []
    for s in schedules:
        stmt = select(BackupScheduleRow)
        stmt = stmt.where(BackupScheduleRow.schedule_id==s.id)
        result = await db.execute(stmt)
        rows = result.scalars().all()
        out.append(
            BackupSchedulePublic(
                ref=str(s.id),
                organization_id=str(s.organization_id) if s.organization_id else None,
                branch_id=str(s.branch_id) if s.branch_id else None,
                env_type=s.env_type,
                rows=[
                    BackupScheduleRowPublic(
                        row_index=r.row_index,
                        interval=r.interval,
                        unit=r.unit,
                        retention=r.retention,
                    )
                    for r in rows
                ],
            )
        )
    return out

# ---------------------------
# List Backups
# ---------------------------
@router.get("/backup/organizations/{org_ref}/")
@router.get("/backup/branches/{branch_ref}/")
async def list_backups(
    org_ref: Optional[Identifier] = None,
    branch_ref: Optional[Identifier] = None,
    env_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
) -> list[BackupPublic]:
    if org_ref:
        stmt = select(BackupEntry).join(Branch).where(Branch.organization_id == org_ref)
        if env_type is not None:
            stmt = stmt.where(Branch.env_type == env_type)
        stmt = select(BackupEntry).join(Branch).where(Branch.organization_id == org_ref)

    elif branch_ref:
        stmt = select(BackupEntry).where(BackupEntry.branch_id == branch_ref)
    else:
        raise HTTPException(status_code=400, detail="Either org-ref or branch-ref needed.")

    result = await db.execute(stmt)
    backups = result.scalars().all()
    if not backups:
        raise HTTPException(status_code=404, detail="No backups found.")

    return [
        BackupPublic(
            id=str(b.id),
            branch_id=str(b.branch_id),
            row_index=b.row_index,
            created_at=b.created_at,
        )
        for b in backups
    ]

# ---------------------------
# Delete Schedule
# ---------------------------
@router.delete("/backup/schedule/{schedule_ref}/")
async def delete_schedule(
    schedule_ref: Optional[Identifier] = None,
    db: AsyncSession = Depends(get_db),
) -> BackupScheduleDeletePublic:
    stmt = select(BackupSchedule)
    stmt = stmt.where(BackupSchedule.id == schedule_ref)

    result = await db.execute(stmt)
    schedule = result.scalars().first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    await db.execute(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))
    await db.execute(delete(NextBackup).where(NextBackup.schedule_id == schedule.id))
    await db.delete(schedule)
    await db.commit()

    return BackupScheduleDeletePublic(status="success", message="Schedule and related data deleted successfully")

# ---------------------------
# Manual Backup
# ---------------------------
@router.post("/backup/branches/{branch_ref}/")
async def manual_backup(branch_ref: Identifier, db: AsyncSession = Depends(get_db)) -> BackupCreatePublic:
    result = await db.execute(select(Branch).where(Branch.id == branch_ref))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")

    backup = BackupEntry(
        branch_id=branch.id,
        row_index=-1,
        created_at=datetime.utcnow(),
        size_bytes=0,
    )
    db.add(backup)
    await db.commit()
    await db.refresh(backup)

    log = BackupLog(
        branch_id=branch.id,
        action="manual-create",
        ts=datetime.utcnow(),
        backup_uuid=str(backup.id),

    )
    db.add(log)
    await db.commit()
    return BackupCreatePublic(status="manual backup created", backup_id=str(backup.id))

# ---------------------------
# Delete Backup
# ---------------------------
@router.delete("/backup/{backup_ref}")
async def delete_backup(backup_ref: Identifier, db: AsyncSession = Depends(get_db)) -> BackupDeletePublic:
    result = await db.execute(select(BackupEntry).where(BackupEntry.id == backup_ref))
    backup = result.scalars().first()
    if not backup:
        raise HTTPException(status_code=404, detail="Backup not found")

    await db.delete(backup)
    await db.commit()

    log = BackupLog(
        branch_id=backup.branch_id,
        backup_uuid=str(backup.id),
        action="manual-delete",
        ts=datetime.utcnow(),
    )
    db.add(log)
    await db.commit()
    return BackupDeletePublic(status="backup deleted")


@router.get("/backup/branches/{branch_id}/info")
async def get_branch_backup_info(
    branch_id: Identifier,
    db: AsyncSession = Depends(get_db),
) -> BackupInfoPublic:
    # 1️⃣ Find the BackupSchedule that applies to this branch
    # (first look for branch-level, then environment/org fallback if applicable)
    stmt = (
        select(BackupSchedule)
        .where(BackupSchedule.branch_id == branch_id)
    )

    result = await db.execute(stmt)
    schedule = result.scalars().first()
    level = "branch"
    nb = None
    # Optionally, if no branch-level schedule found, fall back to org/env-level
    if not schedule:
        # Find branch’s organization and environment (if such relation exists)
        branch_stmt = select(Branch).where(Branch.id == branch_id)
        branch_result = await db.execute(branch_stmt)
        branch = branch_result.scalar_one_or_none()

        if not branch:
            raise HTTPException(status_code=404, detail="Branch not found")

        # Try environment-level schedule
        if branch.environment_type:
            stmt = (
                select(BackupSchedule)
                .where(BackupSchedule.environment_type == branch.environment_type,
                       BackupSchedule.organization_id == branch.organization_id)

            )
            level="environment"
        else:
            stmt = (
                select(BackupSchedule)
                .where(BackupSchedule.organization_id == branch.organization_id)

            )
            level="organization"
        result = await db.execute(stmt)
        schedule = result.scalars().first()

    stmt = (
        select(NextBackup)
        .where(NextBackup.branch_id == branch_id,
                   NextBackup.schedule_id == schedule.id)
    ).order_by(NextBackup.next_at.asc()).limit(1)
    result = await db.execute(stmt)
    nb = result.scalars().first()

    # 2️⃣ If still no schedule → no backup config
    if not schedule:
        raise HTTPException(status_code=404, detail="No backup schedule found for this branch")

    return BackupInfoPublic(
        branch_id=str(branch_id),
        schedule_id=str(schedule.id),
        level=level,
        next_backup=nb.next_at
    )
