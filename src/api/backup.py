import logging
import os
from collections import Counter
from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlmodel import asc, delete, select
from ulid import ULID

from .backup_snapshots import create_branch_snapshot, delete_branch_snapshot
from .db import SessionDep
from .models._util import Identifier
from .models.backups import (
    BackupCreatePublic,
    BackupDeletePublic,
    BackupEntry,
    BackupInfoPublic,
    BackupLog,
    BackupPublic,
    BackupSchedule,
    BackupScheduleCreatePublic,
    BackupScheduleDeletePublic,
    BackupSchedulePublic,
    BackupScheduleRow,
    BackupScheduleRowPublic,
    NextBackup,
)
from .models.branch import Branch
from .models.organization import Organization, OrganizationDep
from .models.project import Project

router = APIRouter()

# ---------------------------
# Constants
# ---------------------------
VOLUME_SNAPSHOT_CLASS = os.environ.get("VOLUME_SNAPSHOT_CLASS", "simplyblock-csi-snapshotclass")
SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
SNAPSHOT_POLL_INTERVAL_SEC = int(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))
MANUAL_BACKUP_TIMEOUT_SEC = int(os.environ.get("MANUAL_BACKUP_TIMEOUT_SEC", "10"))

UNIT_MULTIPLIER = {
    "min": 60,
    "minute": 60,
    "minutes": 60,
    "h": 3600,
    "hour": 3600,
    "hours": 3600,
    "d": 86400,
    "day": 86400,
    "days": 86400,
    "w": 604800,
    "week": 604800,
    "weeks": 604800,
}

VALID_UNITS = set(UNIT_MULTIPLIER.keys())

INTERVAL_LIMITS = {
    "minute": 59,
    "minutes": 59,
    "min": 59,
    "hour": 23,
    "hours": 23,
    "h": 23,
    "day": 6,
    "d": 6,
    "days": 6,
    "week": 12,
    "w": 12,
    "weeks": 12,
}


type ResponseType = Literal["empty", "full"]


# ---------------------------
# Pydantic Schemas
# ---------------------------
class ScheduleRow(BaseModel):
    row_index: int
    interval: int
    unit: str
    retention: int

    @field_validator("unit")
    @classmethod
    def valid_unit(cls, unit) -> Self:
        if unit not in UNIT_MULTIPLIER:
            raise ValueError("Invalid unit")
        return unit

    @model_validator(mode="after")
    def valid_interval(self):
        if self.interval > INTERVAL_LIMITS.get(self.unit, 9999):
            raise ValueError(f"Interval for {self.unit} cannot exceed {INTERVAL_LIMITS.get(self.unit)}")
        return self


def _duplicates(xs):
    return [x for x, count in Counter(xs).items() if count > 1]


class SchedulePayload(BaseModel):
    rows: Annotated[list[ScheduleRow], Field(min_length=0, max_length=10)]
    env_type: str | None = None

    @field_validator("rows")
    @classmethod
    def unique_rows(cls, rows):
        if duplicates := _duplicates((row.interval, row.unit) for row in rows):
            raise ValueError(f"Duplicate row found in schedule: {duplicates}")
        return rows


class BackupScheduleUpdate(SchedulePayload):
    schedule_id: ULID


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------
# Create/Update Schedule
# ---------------------------
async def _lookup_backup_schedule(
    session: SessionDep, organization_id: Identifier | None, branch_id: Identifier | None, payload: SchedulePayload
) -> BackupSchedule | None:
    stmt = select(BackupSchedule)
    if organization_id is not None:
        stmt = stmt.where(BackupSchedule.organization_id == organization_id)
    elif branch_id is not None:
        stmt = stmt.where(BackupSchedule.branch_id == branch_id)
    else:
        raise AssertionError("unreachable")

    if payload.env_type is not None:
        stmt = stmt.where(BackupSchedule.env_type == payload.env_type)

    result = await session.execute(stmt)
    return result.scalars().one_or_none()


BackupScheduleDep = Annotated[BackupSchedule | None, Depends(_lookup_backup_schedule)]


@router.post("/backup/organizations/{organization_id}/schedule")
async def add_org_backup_schedule(
    session: SessionDep, payload: SchedulePayload, organization: OrganizationDep, response: ResponseType = "empty"
) -> BackupScheduleCreatePublic | BackupSchedulePublic:
    return await add_or_replace_backup_schedule(session, payload, organization, None, None, response)


@router.put("/backup/organizations/{organization_id}/schedule")
async def replace_org_backup_schedule(
    session: SessionDep,
    payload: BackupScheduleUpdate,
    organization: OrganizationDep,
    response: ResponseType = "empty",
) -> BackupScheduleCreatePublic | BackupSchedulePublic:
    # Dependency object don't work as they bring in required query parameters
    result = await session.execute(select(BackupSchedule).where(BackupSchedule.id == payload.schedule_id))
    schedule = result.scalars().one_or_none()
    return await add_or_replace_backup_schedule(session, payload, organization, None, schedule, response)


@router.post("/backup/branches/{branch_id}/schedule")
async def add_branch_backup_schedule(
    session: SessionDep,
    payload: SchedulePayload,
    branch_id: ULID,
    response: ResponseType = "empty",
) -> BackupScheduleCreatePublic | BackupSchedulePublic:
    # Dependency object don't work as they bring in required query parameters
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().one_or_none()
    return await add_or_replace_backup_schedule(session, payload, None, branch, None, response)


@router.put("/backup/branches/{branch_id}/schedule")
async def replace_branch_backup_schedule(
    session: SessionDep,
    payload: BackupScheduleUpdate,
    branch_id: ULID,
    response: ResponseType = "empty",
) -> BackupScheduleCreatePublic | BackupSchedulePublic:
    # Dependency object don't work as they bring in required query parameters
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().one_or_none()
    result = await session.execute(select(BackupSchedule).where(BackupSchedule.id == payload.schedule_id))
    schedule = result.scalars().one_or_none()
    return await add_or_replace_backup_schedule(session, payload, None, branch, schedule, response)


async def add_or_replace_backup_schedule(
    session: SessionDep,
    payload: SchedulePayload,
    organization: Organization | None,
    branch: Branch | None,
    schedule: BackupSchedule | None,
    response: ResponseType,
) -> BackupScheduleCreatePublic | BackupSchedulePublic:
    # TODO: @mxsrc will currently throw an HTTP 500 if the unique constraint fails. Please adjust to 409 Conflict.

    if organization is None and branch is None:
        raise HTTPException(status_code=404, detail="Valid branch or organization required.")

    # Max backups validation
    if organization is not None:
        max_allowed = organization.max_backups
        entity_type = "Organization"
        entity_id = organization.id
    elif branch is not None:
        max_allowed = (await branch.awaitable_attrs.project).max_backups
        entity_type = "Branch"
        entity_id = branch.id
    else:
        raise AssertionError("unreachable")

    if (total_retention := sum(row.retention for row in payload.rows)) > max_allowed:
        raise HTTPException(
            status_code=422,
            detail=f"Max Backups {max_allowed} of {entity_type} {entity_id} exceeded: {total_retention}",
        )

    if schedule is not None:
        await session.execute(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))  # type: ignore
        await session.execute(delete(NextBackup).where(NextBackup.schedule_id == schedule.id))  # type: ignore
        await session.execute(delete(BackupSchedule).where(BackupSchedule.id == schedule.id))  # type: ignore
        await session.commit()
        if organization is not None:
            await session.refresh(organization)
        if branch is not None:
            await session.refresh(branch)

    schedule = BackupSchedule(
        organization_id=organization.id if organization is not None else None,
        branch_id=branch.id if branch is not None else None,
        env_type=payload.env_type,
        rows=[
            BackupScheduleRow(
                row_index=row.row_index,
                interval=row.interval,
                unit=row.unit,
                retention=row.retention,
            )
            for row in payload.rows
        ],
    )
    session.add(schedule)
    await session.commit()
    await session.refresh(schedule)

    if response == "full":
        return BackupSchedulePublic(
            id=schedule.id,
            organization_id=schedule.organization_id,
            project_id=branch.project_id if branch else None,
            branch_id=schedule.branch_id,
            env_type=schedule.env_type,
            rows=[
                BackupScheduleRowPublic(
                    row_index=row.row_index,
                    interval=row.interval,
                    unit=row.unit,
                    retention=row.retention,
                )
                for row in schedule.rows
            ],
        )
    return BackupScheduleCreatePublic(status="ok", schedule_id=schedule.id)


# ---------------------------
# List Schedules
# ---------------------------
@router.get("/backup/organizations/{organization_id}/schedule")
async def list_org_schedules(
    session: SessionDep,
    organization_id: Identifier,
    env_type: str | None = None,
) -> list[BackupSchedulePublic]:
    return await list_schedules(session, organization_id, None, env_type)


@router.get("/backup/branches/{branch_id}/schedule")
async def list_branch_schedules(
    session: SessionDep,
    branch_id: Identifier,
    env_type: str | None = None,
) -> list[BackupSchedulePublic]:
    return await list_schedules(session, None, branch_id, env_type)


async def list_schedules(
    session: SessionDep,
    organization_id: Identifier | None,
    branch_id: Identifier | None,
    env_type: str | None,
) -> list[BackupSchedulePublic]:
    stmt = select(BackupSchedule)
    if organization_id:
        stmt = stmt.where(BackupSchedule.organization_id == organization_id)
    if branch_id:
        stmt = stmt.where(BackupSchedule.branch_id == branch_id)
    if env_type is not None:
        stmt = stmt.where(BackupSchedule.env_type == env_type)

    result = await session.execute(stmt)
    schedules = result.scalars().all()
    if not schedules:
        return []

    out: list[BackupSchedulePublic] = []
    for schedule in schedules:
        stmt2 = select(BackupScheduleRow)
        stmt2 = stmt2.where(BackupScheduleRow.schedule_id == schedule.id)
        result2 = await session.execute(stmt2)
        rows = result2.scalars().all()
        out.append(
            BackupSchedulePublic(
                id=schedule.id,
                organization_id=schedule.organization_id if schedule.organization_id else None,
                project_id=(await schedule.awaitable_attrs.branch).project_id if schedule.branch_id else None,
                branch_id=schedule.branch_id if schedule.branch_id else None,
                env_type=schedule.env_type,
                rows=[
                    BackupScheduleRowPublic(
                        row_index=row.row_index,
                        interval=row.interval,
                        unit=row.unit,
                        retention=row.retention,
                    )
                    for row in rows
                ],
            )
        )
    return out


# ---------------------------
# List Backups
# ---------------------------
@router.get("/backup/organizations/{organization_id}/")
async def list_org_backups(
    session: SessionDep,
    organization_id: Identifier | None,
    env_type: str | None = None,
) -> list[BackupPublic]:
    return await list_backups(session, organization_id, None, env_type)


@router.get("/backup/branches/{branch_id}/")
async def list_branch_backups(
    session: SessionDep,
    branch_id: Identifier | None,
    env_type: str | None = None,
) -> list[BackupPublic]:
    return await list_backups(session, None, branch_id, env_type)


async def list_backups(
    session: SessionDep,
    organization_id: Identifier | None = None,
    branch_id: Identifier | None = None,
    env_type: str | None = None,
) -> list[BackupPublic]:
    if organization_id:
        stmt = (
            select(BackupEntry)
            .join(Branch, isouter=True)
            .where(Branch.project_id == Project.id)
            .join(Project, isouter=True)
            .where(Project.organization_id == organization_id)
        )
        if env_type is not None:
            stmt = stmt.where(Branch.env_type == env_type)

    elif branch_id:
        stmt = select(BackupEntry).where(BackupEntry.branch_id == branch_id)
    else:
        raise HTTPException(status_code=400, detail="Either org-ref or branch-ref needed.")

    result = await session.execute(stmt)
    backups = result.scalars().all()
    if not backups:
        return []

    async def backup_mapper(backup: BackupEntry):
        branch = await backup.awaitable_attrs.branch
        project = await branch.awaitable_attrs.project
        return BackupPublic(
            id=backup.id,
            organization_id=project.organization_id,
            project_id=project.id,
            branch_id=backup.branch_id,
            row_index=backup.row_index,
            created_at=backup.created_at,
            size_bytes=backup.size_bytes,
        )

    return [await backup_mapper(b) for b in backups]


# ---------------------------
# Delete Schedule
# ---------------------------
@router.delete("/backup/schedule/{schedule_id}/")
async def delete_schedule(
    session: SessionDep,
    schedule_id: Identifier | None = None,
) -> BackupScheduleDeletePublic:
    stmt = select(BackupSchedule)
    stmt = stmt.where(BackupSchedule.id == schedule_id)

    result = await session.execute(stmt)
    schedule = result.scalars().first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    await session.execute(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))
    await session.execute(delete(NextBackup).where(NextBackup.schedule_id == schedule.id))
    await session.delete(schedule)
    await session.commit()

    return BackupScheduleDeletePublic(status="success", message="Schedule and related data deleted successfully")


# ---------------------------
# Manual Backup
# ---------------------------
@router.post("/backup/branches/{branch_id}/")
async def manual_backup(session: SessionDep, branch_id: Identifier) -> BackupCreatePublic:
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")

    backup_id = ULID()
    recorded_at = datetime.now(UTC)

    try:
        snapshot = await create_branch_snapshot(
            branch.id,
            backup_id=backup_id,
            snapshot_class=VOLUME_SNAPSHOT_CLASS,
            poll_interval=SNAPSHOT_POLL_INTERVAL_SEC,
            label="manual",
            time_limit=MANUAL_BACKUP_TIMEOUT_SEC,
        )
    except Exception as exc:
        logger.exception("Manual backup failed for branch %s within timeout", branch.id)
        raise HTTPException(status_code=500, detail="Manual backup failed") from exc

    backup = BackupEntry(
        id=backup_id,
        branch_id=branch.id,
        row_index=-1,
        created_at=recorded_at,
        size_bytes=snapshot.size_bytes or 0,
        snapshot_name=snapshot.name,
        snapshot_namespace=snapshot.namespace,
        snapshot_content_name=snapshot.content_name,
    )
    session.add(backup)
    await session.flush()
    backup_uuid = str(backup_id)

    log = BackupLog(
        branch_id=branch.id,
        action="manual-create",
        ts=recorded_at,
        backup_uuid=backup_uuid,
    )
    session.add(log)
    await session.commit()
    return BackupCreatePublic(status="manual backup created", backup_id=backup_id)


# ---------------------------
# Delete Backup
# ---------------------------
@router.delete("/backup/{backup_id}")
async def delete_backup(session: SessionDep, backup_id: Identifier) -> BackupDeletePublic:
    result = await session.execute(select(BackupEntry).where(BackupEntry.id == backup_id))
    backup = result.scalars().first()
    if not backup:
        raise HTTPException(status_code=404, detail="Backup not found")

    try:
        await delete_branch_snapshot(
            name=backup.snapshot_name,
            namespace=backup.snapshot_namespace,
            content_name=backup.snapshot_content_name,
            time_limit=SNAPSHOT_TIMEOUT_SEC,
            poll_interval=SNAPSHOT_POLL_INTERVAL_SEC,
        )
    except Exception as exc:
        logger.exception("Failed to delete snapshot for backup %s", backup_id)
        raise HTTPException(status_code=500, detail="Failed to delete backup snapshot") from exc

    await session.delete(backup)

    log = BackupLog(
        branch_id=backup.branch_id,
        backup_uuid=str(backup.id),
        action="manual-delete",
        ts=datetime.now(UTC),
    )
    session.add(log)
    await session.commit()
    return BackupDeletePublic(status="backup deleted")


@router.get("/backup/branches/{branch_id}/info")
async def get_branch_backup_info(
    session: SessionDep,
    branch_id: Identifier,
) -> BackupInfoPublic:
    # 1️⃣ Find the BackupSchedule that applies to this branch
    # (first look for branch-level, then environment/org fallback if applicable)
    stmt = select(BackupSchedule).where(BackupSchedule.branch_id == branch_id)

    result = await session.execute(stmt)
    schedule = result.scalars().one_or_none()
    level = "branch"
    # Optionally, if no branch-level schedule found, fall back to org/env-level
    if not schedule:
        # Find branch’s organization and environment (if such relation exists)
        branch_stmt = select(Branch).where(Branch.id == branch_id)
        branch_result = await session.execute(branch_stmt)
        branch = branch_result.scalar_one_or_none()

        if not branch:
            raise HTTPException(status_code=404, detail="Branch not found")

        # Try environment-level schedule
        if branch.environment_type:
            stmt = select(BackupSchedule).where(
                BackupSchedule.env_type == branch.env_type,
                BackupSchedule.organization_id == branch.organization_id,
            )
            level = "environment"
        else:
            stmt = select(BackupSchedule).where(BackupSchedule.organization_id == branch.organization_id)
            level = "organization"
        result = await session.execute(stmt)
        schedule = result.scalars().one()

    stmt2 = (
        (select(NextBackup).where(NextBackup.branch_id == branch_id, NextBackup.schedule_id == schedule.id))
        .order_by(asc(NextBackup.next_at))
        .limit(1)
    )
    result2 = await session.execute(stmt2)
    nb2 = result2.scalars().one()

    # 2️⃣ If still no schedule → no backup config
    if not schedule:
        raise HTTPException(status_code=404, detail="No backup schedule found for this branch")

    return BackupInfoPublic(branch_id=branch_id, schedule_id=schedule.id, level=level, next_backup=nb2.next_at)
