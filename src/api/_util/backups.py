from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import HTTPException
from sqlalchemy import func
from sqlmodel import delete, select

from ..models.backups import BackupSchedule, BackupScheduleRow, NextBackup
from ..models.branch import Branch

if TYPE_CHECKING:
    from ..._util import Identifier
    from ..dependencies import SessionDep
    from ..models.organization import Organization
    from ..models.project import Project


async def copy_branch_backup_schedules(
    session: SessionDep,
    source_branch_id: Identifier,
    target: Branch,
) -> None:
    """Clone backup schedules (and their rows) from source branch to target branch."""
    result = await session.exec(select(BackupSchedule).where(BackupSchedule.branch_id == source_branch_id))
    schedules = list(result.all())
    if not schedules:
        return

    for schedule in schedules:
        rows = list(await schedule.awaitable_attrs.rows)
        rows.sort(key=lambda row: row.row_index)
        session.add(
            BackupSchedule(
                organization_id=schedule.organization_id,
                branch_id=target.id,
                env_type=schedule.env_type,
                rows=[
                    BackupScheduleRow(
                        row_index=row.row_index,
                        interval=row.interval,
                        unit=row.unit,
                        retention=row.retention,
                    )
                    for row in rows
                ],
            )
        )


async def _validate_project_retention_budget(
    session: SessionDep,
    project: Project | None,
    schedule: BackupSchedule | None,
    new_retention: int,
) -> None:
    if project is None:
        return

    stmt = (
        select(func.coalesce(func.sum(BackupScheduleRow.retention), 0))
        .select_from(BackupScheduleRow)
        .join(BackupSchedule)
        .join(Branch)
        .where(Branch.project_id == project.id)
    )
    if schedule is not None:
        stmt = stmt.where(BackupScheduleRow.schedule_id != schedule.id)

    result = await session.execute(stmt)
    existing_project_retention = result.scalar_one() or 0
    combined_retention = existing_project_retention + new_retention
    if combined_retention > project.max_backups:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Project {project.id} max backups {project.max_backups} exceeded by branch schedule:"
                f" total retention {combined_retention}"
            ),
        )


async def _remove_existing_schedule(
    session: SessionDep,
    schedule: BackupSchedule | None,
    *,
    organization: Organization | None,
    branch: Branch | None,
) -> None:
    if schedule is None:
        return

    await session.execute(delete(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))  # type: ignore
    await session.execute(delete(NextBackup).where(NextBackup.schedule_id == schedule.id))  # type: ignore
    await session.execute(delete(BackupSchedule).where(BackupSchedule.id == schedule.id))  # type: ignore
    await session.commit()
    if organization is not None:
        await session.refresh(organization)
    if branch is not None:
        await session.refresh(branch)
