import asyncio
import logging
import os
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel, asc, select
from ulid import ULID

from ..check_branch_status import get_branch_status
from .backup_snapshots import create_branch_snapshot, delete_branch_snapshot
from .models.backups import (
    BackupEntry,
    BackupLog,
    BackupSchedule,
    BackupScheduleRow,
    NextBackup,
)
from .models.branch import Branch
from .models.organization import Organization
from .models.project import Project
from .settings import settings

# ---------------------------
# Config
# ---------------------------
VOLUME_SNAPSHOT_CLASS = os.environ.get("VOLUME_SNAPSHOT_CLASS", "csi-snapshot-class")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
SNAPSHOT_POLL_INTERVAL_SEC = int(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

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


def interval_seconds(interval: int, unit: str) -> int:
    return interval * UNIT_MULTIPLIER[unit.lower()]


# ---------------------------
# Async DB setup
# ---------------------------
engine = create_async_engine(
    str(settings.postgres_url),
    echo=False,
    pool_pre_ping=True,
    pool_recycle=3600,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@asynccontextmanager
async def get_db() -> AsyncGenerator[AsyncSession]:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


# ---------------------------
# Backup Monitor
# ---------------------------
class BackupMonitor:
    def __init__(self):
        self.branch_locks: dict[ULID, asyncio.Lock] = {}

    def get_branch_lock(self, branch_id: ULID) -> asyncio.Lock:
        if branch_id not in self.branch_locks:
            self.branch_locks[branch_id] = asyncio.Lock()
        return self.branch_locks[branch_id]

    async def run_once(self, db: AsyncSession):
        now = datetime.now(UTC)

        # to-do: must match witch branch status!!! Only online branches must be processed!!!

        result = await db.execute(select(Branch))
        branches = result.scalars().all()
        logger.info("Found %d branches", len(branches))

        for branch in branches:
            status = await get_branch_status(branch.id)
            if status == "ACTIVE_HEALTHY":
                try:
                    await self.process_branch(db, branch, now)
                except Exception:
                    logger.exception("Error processing branch %s", branch.id)

    async def process_branch(self, db: AsyncSession, branch: Branch, now: datetime):
        branch_local = await db.get(Branch, branch.id, populate_existing=True)
        if not branch_local:
            logger.debug("Branch %s disappeared", branch.id)
            return

        schedule = await self.resolve_schedule(db, branch_local)
        if not schedule:
            logger.info("No schedule for branch %s", branch_local.id)
            return

        rows = await self._get_schedule_rows(db, schedule.id)
        rows_by_index = {row.row_index: row for row in rows}

        await self._ensure_next_backups(db, branch_local, schedule.id, rows_by_index, now)
        await db.commit()
        await db.refresh(branch_local)

        await self._process_due_backups(db, branch_local, rows_by_index, now)
        await self._enforce_global_max_backups(db, branch_local)

    async def _get_schedule_rows(self, db: AsyncSession, schedule_id: ULID) -> list[BackupScheduleRow]:
        result = await db.execute(select(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule_id))
        return list(result.scalars().all())

    async def _ensure_next_backups(
        self,
        db: AsyncSession,
        branch: Branch,
        schedule_id: ULID,
        rows_by_index: dict[int, BackupScheduleRow],
        now: datetime,
    ):
        for row in rows_by_index.values():
            stmt = select(NextBackup).where(NextBackup.branch_id == branch.id, NextBackup.row_index == row.row_index)
            res = await db.execute(stmt)
            nb = res.scalar_one_or_none()
            if nb is None:
                nb = NextBackup(
                    branch_id=branch.id,
                    schedule_id=schedule_id,
                    row_index=row.row_index,
                    next_at=now + timedelta(seconds=interval_seconds(row.interval, row.unit)),
                )
                db.add(nb)

    async def _process_due_backups(
        self,
        db: AsyncSession,
        branch: Branch,
        rows_by_index: dict[int, BackupScheduleRow],
        now: datetime,
    ):
        result = await db.execute(select(NextBackup).where(NextBackup.branch_id == branch.id))
        next_backups = result.scalars().all()

        for nb in next_backups:
            if nb.next_at > now:
                continue

            row = rows_by_index.get(nb.row_index)
            if not row:
                continue

            lock = self.get_branch_lock(branch.id)
            if lock.locked():
                logger.debug(
                    "Skipping snapshot for %s row %d because another worker holds lock",
                    branch.id,
                    nb.row_index,
                )
                continue

            async with lock:
                await self.execute_backup(db, branch, row, nb)

    async def _delete_many(
        self,
        db: AsyncSession,
        backups: Sequence[BackupEntry],
        branch: Branch,
        *,
        failure_template: str = "Failed to delete snapshot for backup {backup_id} (branch {branch_id})",
        log_action: str | None = None,
    ) -> int:
        if not backups:
            return 0

        deleted = 0
        for backup in backups:
            try:
                await delete_branch_snapshot(
                    name=backup.snapshot_name,
                    namespace=backup.snapshot_namespace,
                    content_name=backup.snapshot_content_name,
                    time_limit=SNAPSHOT_TIMEOUT_SEC,
                    poll_interval=SNAPSHOT_POLL_INTERVAL_SEC,
                )
            except Exception:
                context = {
                    "backup_id": backup.id,
                    "branch_id": branch.id,
                    "row_index": getattr(backup, "row_index", None),
                }
                try:
                    message = failure_template.format(**context)
                except KeyError:
                    message = failure_template
                logger.exception(message)
                continue

            if log_action:
                log_entry = BackupLog(
                    branch_id=branch.id,
                    backup_uuid=str(backup.id),
                    action=log_action,
                    ts=datetime.now(UTC),
                )
                db.add(log_entry)

            await db.delete(backup)
            deleted += 1

        if deleted:
            await db.commit()

        return deleted

    async def _enforce_global_max_backups(self, db: AsyncSession, branch: Branch):
        stmt_project = select(Project).where(Project.id == branch.project_id)
        project_res = await db.execute(stmt_project)
        project = project_res.scalars().one()

        stmt_org = select(Organization).where(Organization.id == project.organization_id)
        org_res = await db.execute(stmt_org)
        organization = org_res.scalars().one()

        max_backups = min(project.max_backups, organization.max_backups)

        stmt_backups = (
            select(BackupEntry).where(BackupEntry.branch_id == branch.id).order_by(asc(BackupEntry.created_at))
        )
        backup_res = await db.execute(stmt_backups)
        entry_rows = backup_res.scalars().all()
        num_rows = len(entry_rows)

        to_delete = entry_rows[: num_rows - max_backups] if num_rows > max_backups else []
        await self._delete_many(db, to_delete, branch)

    async def resolve_schedule(self, db: AsyncSession, branch: Branch) -> BackupSchedule | None:
        project = await branch.awaitable_attrs.project

        stmt = select(BackupSchedule).where(BackupSchedule.branch_id == branch.id)
        res = await db.execute(stmt)
        schedule = res.scalar_one_or_none()
        if schedule:
            return schedule

        stmt2 = select(BackupSchedule).where(
            BackupSchedule.organization_id == project.organization_id,
            BackupSchedule.env_type == branch.env_type,
            BackupSchedule.branch_id.is_(None),  # type: ignore[union-attr]
        )
        res2 = await db.execute(stmt2)
        schedule = res2.scalar_one_or_none()
        if schedule:
            return schedule

        stmt1 = select(BackupSchedule).where(
            BackupSchedule.organization_id == project.organization_id,
            BackupSchedule.branch_id.is_(None),  # type: ignore[union-attr]
            BackupSchedule.env_type.is_(None),  # type: ignore[union-attr]
        )
        res1 = await db.execute(stmt1)
        schedule = res1.scalar_one_or_none()
        if schedule is None:
            logger.debug(
                "No backup schedule found for branch %s (env %s, org %s)",
                branch.id,
                branch.env_type,
                project.organization_id,
            )
        return schedule

    async def execute_backup(self, db: AsyncSession, branch: Branch, row: BackupScheduleRow, nb: NextBackup):
        next_due = nb.next_at + timedelta(seconds=interval_seconds(row.interval, row.unit))
        backup_id = ULID()

        try:
            snapshot = await create_branch_snapshot(
                branch.id,
                backup_id=backup_id,
                snapshot_class=VOLUME_SNAPSHOT_CLASS,
                poll_interval=SNAPSHOT_POLL_INTERVAL_SEC,
                label=f"row-{row.row_index}",
                time_limit=SNAPSHOT_TIMEOUT_SEC,
            )
        except Exception:
            nb.next_at = next_due
            db.add(nb)
            await db.commit()
            logger.exception("Failed to create backup snapshot for branch %s row %d", branch.id, row.row_index)
            return

        created_at = datetime.now(UTC)
        be = BackupEntry(
            id=backup_id,
            branch_id=branch.id,
            row_index=row.row_index,
            created_at=created_at,
            size_bytes=snapshot.size_bytes or 0,
            snapshot_name=snapshot.name,
            snapshot_namespace=snapshot.namespace,
            snapshot_content_name=snapshot.content_name,
        )
        db.add(be)
        await db.flush()

        log_entry = BackupLog(
            branch_id=branch.id,
            backup_uuid=str(backup_id),
            action="taken",
            ts=created_at,
        )
        db.add(log_entry)

        nb.next_at = next_due
        db.add(nb)
        await db.commit()

        await self.prune_backups(db, branch, row)

        logger.info(
            "Backup created %s for branch %s row %d (snapshot=%s/%s)",
            be.id,
            branch.id,
            row.row_index,
            snapshot.namespace,
            snapshot.name,
        )

    async def prune_backups(self, db: AsyncSession, branch: Branch, row: BackupScheduleRow):
        result = await db.execute(
            select(BackupEntry)
            .where(BackupEntry.branch_id == branch.id, BackupEntry.row_index == row.row_index)
            .order_by(asc(BackupEntry.created_at))
        )
        backups = result.scalars().all()
        if len(backups) <= row.retention:
            return

        to_delete = backups[: len(backups) - row.retention]
        deleted = await self._delete_many(
            db,
            to_delete,
            branch,
            failure_template="Failed to delete snapshot for backup {backup_id} (branch {branch_id} row {row_index})",
            log_action="delete",
        )

        if deleted:
            logger.info("Pruned %d old backups for branch %s row %d", deleted, branch.id, row.row_index)


# in main.py or backupmonitor.py
async def run_backup_monitor():
    monitor = BackupMonitor()
    while True:
        try:
            async with AsyncSessionLocal() as db:
                await monitor.run_once(db)
        except Exception:
            logger.exception("Error running backup monitor iteration")
        await asyncio.sleep(POLL_INTERVAL)
