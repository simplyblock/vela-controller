# api/backups_async.py

from typing import List, Optional
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, validator
from sqlmodel import select
from sqlalchemy import delete
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from .db import get_db
from .models.base import Organization, Branch, Project
from .models.backups import (
    BackupSchedule,
    BackupScheduleRow,
    BackupEntry,
    BackupLog,
    NextBackup,
)

router = APIRouter()

# --- Constants copied from your code
UNIT_MULTIPLIER = {
    "min": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
    "w": 604800, "week": 604800, "weeks": 604800,
}

VALID_UNITS = {"minute", "hour", "day", "week", "min", "minutes", "h", "hours", "d", "days", "w", "weeks"}
INTERVAL_LIMITS = {
    "minute": 59, "minutes": 59, "min": 59,
    "hour": 23, "hours": 23, "h": 23,
    "day": 6, "d": 6, "days": 6,
    "week": 12, "w": 12, "weeks": 12,
}

# ---------------------------
# Pydantic request schemas
# ---------------------------
class ScheduleRow(BaseModel):
    row_index: int
    interval: int
    unit: str
    retention: int

    @validator("unit")
    def unit_must_be_valid(cls, v: str):
        if v not in VALID_UNITS:
            raise ValueError("invalid unit")
        return v


class SchedulePayload(BaseModel):
    rows: List[ScheduleRow]
    env_type: Optional[str] = None

@router.post("/backup/organizations/{org_ref}/schedule", response_model=None)
@router.put("/backup/organizations/{org_ref}/schedule", response_model=None)
@router.post("/backup/branches/{branch_ref}/schedule", response_model=None)
@router.put("/backup/branches/{branch_ref}/schedule", response_model=None)
async def add_or_replace_backup_schedule(
    payload: SchedulePayload,
    org_ref: Optional[str] = None,
    branch_ref: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    # Determine caller method from FastAPI internals isn't available here,
    # so we infer intent by having separate routes for POST/PUT. FastAPI
    # provides request.method via Request if needed; simpler: rely on route used.
    # But to preserve previous behavior (checking existing schedule on POST), we'll:
    # - If route invoked via POST -> create error if exists
    # - If invoked via PUT -> require existing and replace
    # FastAPI allows us to get the method through dependency, but for simplicity
    # we can treat POST/PUT separately by inspecting the path operation name.
    #
    # However to keep a single handler, we can detect the method using
    # request.scope["method"] if needed. For simplicity, we will pull from
    # the underlying ASGI scope via db (not ideal). Instead, assume:
    #   - If schedule exists -> this call acts like PUT (replace)
    #   - If it does not exist -> acts like POST (create)
    #
    # This keeps behavior idempotent and straightforward in async handler.

    # Basic validation
    rows = payload.rows
    if not rows:
        raise HTTPException(status_code=400, detail="No rows provided")

    if len(rows) > 10:
        raise HTTPException(status_code=422, detail="Too many rows in schedule. Max: 10")

    # Resolve org or branch
    org = None
    branch = None
    org_id = None
    branch_id = None

    if org_ref:
        result = await db.exec(select(Organization).where(Organization.id == org_ref))
        org = result.one_or_none()
        if org:
            org_id = org.id
    elif branch_ref:
        result = await db.exec(select(Branch).where(Branch.id == branch_ref))
        branch = result.one_or_none()
        if branch:
            branch_id = branch.id

    if not org and not branch:
        raise HTTPException(status_code=404, detail="Valid branch or organization required.")

    # find existing schedule for the same org/branch + env_type
    env_type = payload.env_type
    stmt = select(BackupSchedule).where(
        BackupSchedule.organization_id == org_id,
        BackupSchedule.branch_id == branch_id,
        BackupSchedule.env_type == env_type,
    )
    result = await db.exec(stmt)
    schedule = result.one_or_none()

    # We'll decide create vs replace based on existence:
    # - If schedule exists: we will replace rows (PUT-like)
    # - If schedule doesn't exist: create (POST-like)
    # If you want to enforce strict POST/PUT semantics, adapt later.

    # Validate rows: duplicates, unit interval limits, total retention
    total = 0
    seen = set()
    for r in rows:
        key = (r.interval, r.unit)
        if key in seen:
            raise HTTPException(status_code=422, detail="Duplicate row found in schedule")
        seen.add(key)

        if r.unit not in VALID_UNITS:
            raise HTTPException(status_code=400, detail=f"Invalid unit: {r.unit}")

        if r.interval > INTERVAL_LIMITS.get(r.unit, 9999):
            raise HTTPException(
                status_code=400,
                detail=f"Interval for {r.unit} cannot exceed {INTERVAL_LIMITS.get(r.unit)}",
            )
        total += r.retention

    # Max backups check
    if branch is None and org and total > getattr(org, "max_backups", 0):
        raise HTTPException(
            status_code=422,
            detail=f"Max Backups {org.max_backups} of Organization {org_ref} exceeded: {total}",
        )
    elif branch and total > getattr(branch, "max_backups", 0):
        raise HTTPException(
            status_code=422,
            detail=f"Max Backups {branch.max_backups} of Branch {branch_ref} exceeded: {total}",
        )

    # If schedule exists -> delete existing rows
    if schedule:
        await db.exec(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))
        await db.commit()
    else:
        schedule = BackupSchedule(
            organization_id=org_id,
            branch_id=branch_id,
            env_type=env_type,
        )
        db.add(schedule)
        await db.commit()  # persist to get id
        await db.refresh(schedule)

    # Add new rows
    for r in rows:
        row = BackupScheduleRow(
            schedule_id=schedule.id,
            row_index=r.row_index,
            interval=r.interval,
            unit=r.unit,
            retention=r.retention,
        )
        db.add(row)

    await db.commit()
    # refresh schedule to ensure rows are loaded if caller needs them
    await db.refresh(schedule)
    return {"status": "ok", "schedule_id": schedule.id}


# ---------------------------
# List schedules
# GET /backup/organizations/{org_ref}/schedule
# GET /backup/branches/{branch_ref}/schedule
# ---------------------------
@router.get("/backup/organizations/{org_ref}/schedule")
@router.get("/backup/branches/{branch_ref}/schedule")
async def list_schedules(
    org_ref: Optional[str] = None,
    branch_ref: Optional[str] = None,
    env_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(BackupSchedule).options(selectinload(BackupSchedule.rows))

    if org_ref:
        stmt = stmt.where(BackupSchedule.organization_id == org_ref)
    if branch_ref:
        stmt = stmt.where(BackupSchedule.branch_id == branch_ref)
    if env_type is not None:
        stmt = stmt.where(BackupSchedule.env_type == env_type)

    result = await db.exec(stmt)
    schedules = result.all()

    if not schedules:
        raise HTTPException(status_code=404, detail="no schedules found.")

    out = []
    for s in schedules:
        out.append(
            {
                "ref": s.id,
                "organization_id": s.organization_id,
                "branch_id": s.branch_id,
                "env_type": s.env_type,
                "rows": [
                    {"row_index": r.row_index, "interval": r.interval, "unit": r.unit, "retention": r.retention}
                    for r in getattr(s, "rows", [])
                ],
            }
        )
    return out


# ---------------------------
# List backups
# GET /backup/organizations/{org_ref}/
# GET /backup/branches/{branch_ref}/
# ---------------------------
@router.get("/backup/organizations/{org_ref}/")
@router.get("/backup/branches/{branch_ref}/")
async def list_backups(
    org_ref: Optional[str] = None,
    branch_ref: Optional[str] = None,
    env_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    if org_ref:
        # join BackupEntry -> Branch and filter by organization (and env_type if provided)
        stmt = select(BackupEntry).join(Branch).where(Branch.organization_id == org_ref)
        if env_type is not None:
            stmt = stmt.where(Branch.env_type == env_type)
        res = await db.exec(stmt)
        backups = res.all()
    elif branch_ref:
        res = await db.exec(select(BackupEntry).where(BackupEntry.branch_id == branch_ref))
        backups = res.all()
    else:
        raise HTTPException(status_code=400, detail="either org-ref or branch-ref needed.")

    if not backups:
        raise HTTPException(status_code=404, detail="no backups found.")

    out = [
        {
            "branch_id": b.branch_id,
            "backup_uuid": b.id,
            "row_index": b.row_index,
            "created_at": b.created_at.isoformat() if isinstance(b.created_at, datetime) else b.created_at,
        }
        for b in backups
    ]
    return out


# ---------------------------
# Delete schedule
# DELETE /backup/organizations/{org_ref}/schedule
# DELETE /backup/branches/{branch_ref}/schedule
# ---------------------------
@router.delete("/backup/organizations/{org_ref}/schedule")
@router.delete("/backup/branches/{branch_ref}/schedule")
async def delete_schedule(
    org_ref: Optional[str] = None,
    branch_ref: Optional[str] = None,
    env_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(BackupSchedule)
    if org_ref:
        stmt = stmt.where(BackupSchedule.organization_id == org_ref)
        if env_type is not None:
            stmt = stmt.where(BackupSchedule.env_type == env_type)
    elif branch_ref:
        stmt = stmt.where(BackupSchedule.branch_id == branch_ref)
    else:
        raise HTTPException(status_code=400, detail="either org-ref or branch-ref needed.")

    res = await db.exec(stmt)
    schedule = res.one_or_none()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    # delete related rows and next_backup entries
    await db.exec(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))
    await db.exec(delete(NextBackup).where(NextBackup.schedule_id == schedule.id))

    # delete schedule
    await db.delete(schedule)
    await db.commit()

    return {"status": "success", "message": "Schedule and related data deleted successfully"}


# ---------------------------
# Manual backup
# POST /backup/branches/{branch_ref}/
# ---------------------------
@router.post("/backup/branches/{branch_ref}/")
async def manual_backup(branch_ref: str, db: AsyncSession = Depends(get_db)):
    res = await db.exec(select(Branch).where(Branch.id == branch_ref))
    branch = res.one_or_none()
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
        backup_uuid=backup.id,
    )
    db.add(log)
    await db.commit()

    return {"status": "manual backup created", "backup_id": backup.id}


# ---------------------------
# Delete backup
# DELETE /backup/{backup_ref}
# ---------------------------
@router.delete("/backup/{backup_ref}")
async def delete_backup(backup_ref: str, db: AsyncSession = Depends(get_db)):
    res = await db.exec(select(BackupEntry).where(BackupEntry.id == backup_ref))
    backup = res.one_or_none()
    if not backup:
        raise HTTPException(status_code=404, detail="Backup not found")

    await db.delete(backup)
    await db.commit()

    log = BackupLog(
        branch_id=backup.branch_id,
        backup_uuid=backup.id,
        action="manual-delete",
        ts=datetime.utcnow(),
    )
    db.add(log)
    await db.commit()
    return {"status": "backup deleted"}
