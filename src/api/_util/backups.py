from __future__ import annotations

from typing import TYPE_CHECKING

from sqlmodel import select

from ..models.backups import BackupSchedule, BackupScheduleRow

if TYPE_CHECKING:
    from ..._util import Identifier
    from ..db import SessionDep
    from ..models.branch import Branch


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
