import logging
import os
from datetime import datetime

from fastapi import APIRouter, Depends, logger, Request, HTTPException
from pydantic import BaseModel, validator
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

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
    def unit_must_be_valid(self, v: str):
        if v not in VALID_UNITS:
            raise ValueError("Invalid unit")
        return v


class SchedulePayload(BaseModel):
    rows: list[ScheduleRow]
    env_type: str | None = None


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


# ---------------------------
# Create/Update Schedule
# ---------------------------
@router.post("/backup/organizations/{organization_id}/schedule")
@router.put("/backup/organizations/{organization_id}/schedule")
async def add_or_replace_org_backup_schedule(
        payload: SchedulePayload,
        organization_id: Identifier,
        db: AsyncSession = Depends(get_db),
        request: Request = None,
) -> BackupScheduleCreatePublic:
    return await add_or_replace_backup_schedule(payload, organization_id, None, db, request)


@router.post("/backup/branches/{branch_id}/schedule")
@router.put("/backup/branches/{branch_id}/schedule")
async def add_or_replace_branch_backup_schedule(
        payload: SchedulePayload,
        branch_id: Identifier,
        db: AsyncSession = Depends(get_db),
        request: Request = None,
) -> BackupScheduleCreatePublic:
    return await add_or_replace_backup_schedule(payload, None, branch_id, db, request)


async def add_or_replace_backup_schedule(
        payload: SchedulePayload,
        organization_id: Identifier | None,
        branch_id: Identifier | None,
        db: AsyncSession = Depends(get_db),
        request: Request = None,
) -> BackupScheduleCreatePublic:
    # TODO: @mxsrc will currently throw an HTTP 500 if the unique constraint fails. Please adjust to 409 Conflict.
    if not payload.rows:
        raise HTTPException(status_code=400, detail="No rows provided")
    if len(payload.rows) > 10:
        raise HTTPException(status_code=422, detail="Too many rows in schedule. Max: 10")

    # Resolve organization or branch
    org = branch = project = None

    if organization_id:
        result = await db.execute(select(Organization).where(Organization.id == organization_id))
        org = result.scalars().first()
        if org:
            logger.info("org-level backup:", str(organization_id))
    elif branch_id:
        result = await db.execute(select(Branch).where(Branch.id == branch_id))
        branch = result.scalars().first()
        if branch:
            result = await db.execute(select(Project).where(Project.id == branch.project_id))
            project = result.scalars().first()
            logger.info("branch-level backup:", str(branch_id))

    if not org and not branch:
        raise HTTPException(status_code=404, detail="Valid branch or organization required.")

    # Find existing schedule and eager-load rows
    schedule = None
    env_type = payload.env_type
    if request.method == "PUT":
        if env_type:
            stmt = (
                select(BackupSchedule)
                .where(
                    BackupSchedule.organization_id == organization_id,
                    BackupSchedule.branch_id == branch_id,
                    BackupSchedule.env_type == payload.env_type
                )
            )
        else:
            stmt = (
                select(BackupSchedule)
                .where(
                    BackupSchedule.organization_id == organization_id,
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
        entity_ref = organization_id if org else branch_id
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
        if branch_id:
            await db.execute(delete(NextBackup).where(NextBackup.branch_id == branch_id))
        elif env_type is not None:
            stmt = delete(NextBackup).where(
                NextBackup.branch_id.in_(
                    select(Branch.id).where(Branch.env_type == env_type)
                )
            )
            await db.execute(stmt)
            await db.commit()
        schedule = BackupSchedule(
            organization_id=organization_id,
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
@router.get("/backup/organizations/{organization_id}/schedule")
async def list_org_schedules(
        organization_id: Identifier,
        env_type: str | None = None,
        db: AsyncSession = Depends(get_db),
) -> list[BackupSchedulePublic]:
    return await list_schedules(organization_id, None, env_type, db)


@router.get("/backup/branches/{branch_id}/schedule")
async def list_branch_schedules(
        branch_id: Identifier,
        env_type: str | None = None,
        db: AsyncSession = Depends(get_db),
) -> list[BackupSchedulePublic]:
    return await list_schedules(None, branch_id, env_type, db)


async def list_schedules(
        organization_id: Identifier | None,
        branch_id: Identifier | None,
        env_type: str | None,
        db: AsyncSession = Depends(get_db),
) -> list[BackupSchedulePublic]:
    stmt = select(BackupSchedule)
    if organization_id:
        stmt = stmt.where(BackupSchedule.organization_id == organization_id)
    if branch_id:
        stmt = stmt.where(BackupSchedule.branch_id == branch_id)
    if env_type is not None:
        stmt = stmt.where(BackupSchedule.env_type == env_type)

    result = await db.execute(stmt)
    schedules = result.scalars().all()
    if not schedules:
        raise HTTPException(status_code=404, detail="No schedules found.")

    out: list[BackupSchedulePublic] = []
    for s in schedules:
        stmt = select(BackupScheduleRow)
        stmt = stmt.where(BackupScheduleRow.schedule_id == s.id)
        result = await db.execute(stmt)
        rows = result.scalars().all()
        out.append(
            BackupSchedulePublic(
                id=str(s.id),
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
@router.get("/backup/organizations/{organization_id}/")
async def list_org_backups(
        organization_id: Identifier | None,
        env_type: str | None = None,
        db: AsyncSession = Depends(get_db),
) -> list[BackupPublic]:
    return await list_backups(organization_id, None, env_type, db)


@router.get("/backup/branches/{branch_id}/")
async def list_branch_backups(
        branch_id: Identifier | None,
        env_type: str | None = None,
        db: AsyncSession = Depends(get_db),
) -> list[BackupPublic]:
    return await list_backups(None, branch_id, env_type, db)


async def list_backups(
        organization_id: Identifier | None = None,
        branch_id: Identifier | None = None,
        env_type: str | None = None,
        db: AsyncSession = Depends(get_db),
) -> list[BackupPublic]:
    if organization_id:
        stmt = (select(BackupEntry)
                .join(Branch, isouter=True).where(Branch.project_id == Project.id)
                .join(Project, isouter=True).where(Project.organization_id == organization_id))
        if env_type is not None:
            stmt = stmt.where(Branch.env_type == env_type)

    elif branch_id:
        stmt = select(BackupEntry).where(BackupEntry.branch_id == branch_id)
    else:
        raise HTTPException(status_code=400, detail="Either org-ref or branch-ref needed.")

    result = await db.execute(stmt)
    backups = result.scalars().all()
    if not backups:
        raise HTTPException(status_code=404, detail="No backups found.")

    async def backup_mapper(backup: BackupEntry):
        branch = await backup.awaitable_attrs.branch
        project = await branch.awaitable_attrs.project
        return BackupPublic(
            id=str(backup.id),
            organization_id=project.organization_id,
            project_id=project.id,
            branch_id=backup.branch_id,
            row_index=backup.row_index,
            created_at=backup.created_at,
        )

    return [
        await backup_mapper(b) for b in backups
    ]


# ---------------------------
# Delete Schedule
# ---------------------------
@router.delete("/backup/schedule/{schedule_id}/")
async def delete_schedule(
        schedule_id: Identifier | None = None,
        db: AsyncSession = Depends(get_db),
) -> BackupScheduleDeletePublic:
    stmt = select(BackupSchedule)
    stmt = stmt.where(BackupSchedule.id == schedule_id)

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
@router.post("/backup/branches/{branch_id}/")
async def manual_backup(branch_id: Identifier, db: AsyncSession = Depends(get_db)) -> BackupCreatePublic:
    result = await db.execute(select(Branch).where(Branch.id == branch_id))
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
@router.delete("/backup/{backup_id}")
async def delete_backup(backup_id: Identifier, db: AsyncSession = Depends(get_db)) -> BackupDeletePublic:
    result = await db.execute(select(BackupEntry).where(BackupEntry.id == backup_id))
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
            level = "environment"
        else:
            stmt = (
                select(BackupSchedule)
                .where(BackupSchedule.organization_id == branch.organization_id)

            )
            level = "organization"
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
