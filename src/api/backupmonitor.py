import asyncio
import logging
import os
from datetime import datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel, asc, select
from ulid import ULID

from ..check_branch_status import get_branch_status
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
K8S_NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")
VOLUME_SNAPSHOT_CLASS = os.environ.get("VOLUME_SNAPSHOT_CLASS", "csi-snapshot-class")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))

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


async def get_db() -> AsyncSession:
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
        now = datetime.utcnow()

        # to-do: must match witch branch status!!! Only online branches must be processed!!!

        result = await db.execute(select(Branch))
        branches = result.scalars().all()
        logger.info("Found %d branches", len(branches))

        for branch in branches:
            status = await get_branch_status(branch)
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

        # Load all schedule rows
        result = await db.execute(select(BackupScheduleRow).where(BackupScheduleRow.schedule_id == schedule.id))
        rows = result.scalars().all()

        # Ensure NextBackup exists for all schedule rows
        for row in rows:
            stmt = select(NextBackup).where(
                NextBackup.branch_id == branch_local.id, NextBackup.row_index == row.row_index
            )
            res = await db.execute(stmt)
            nb = res.scalar_one_or_none()
            if nb is None:
                nb = NextBackup(
                    branch_id=branch_local.id,
                    schedule_id=schedule.id,
                    row_index=row.row_index,
                    next_at=now + timedelta(seconds=interval_seconds(row.interval, row.unit)),
                )
                db.add(nb)
        await db.commit()
        await db.refresh(branch_local)

        # Process due NextBackup entries
        result = await db.execute(select(NextBackup).where(NextBackup.branch_id == branch_local.id))
        next_backups = result.scalars().all()

        for nb in next_backups:
            if nb.next_at <= now:
                stmt_row = select(BackupScheduleRow).where(
                    BackupScheduleRow.schedule_id == schedule.id, BackupScheduleRow.row_index == nb.row_index
                )
                row_res = await db.execute(stmt_row)
                row = row_res.scalar_one_or_none()
                if not row:
                    continue

                lock = self.get_branch_lock(branch_local.id)
                if not lock.locked():
                    async with lock:
                        await self.execute_backup(db, branch_local, row, nb)
                else:
                    logger.debug(
                        "Skipping snapshot for %s row %d because another worker holds lock",
                        branch_local.id,
                        nb.row_index,
                    )
        stmt1 = select(Project).where(Project.id == branch.project_id)
        entries = await db.execute(stmt1)
        proj = entries.scalars().first()

        stmt2 = select(Organization).where(Organization.id == (await branch.awaitable_attrs.project).organization_id)
        entries = await db.execute(stmt2)
        org = entries.scalars().first()

        max_backups = min(proj.max_backups, org.max_backups)

        stmt = select(BackupEntry).where(BackupEntry.branch_id == branch.id).order_by(asc(BackupEntry.created_at))
        entries = await db.execute(stmt)
        entry_rows = entries.scalars().all()
        num_rows = len(entry_rows)

        to_delete = entry_rows[: num_rows - max_backups] if num_rows > max_backups else []
        for row in to_delete:
            await db.delete(row)  # async delete
        await db.commit()

    async def resolve_schedule(self, db: AsyncSession, branch: Branch) -> BackupSchedule:
        stmt = select(BackupSchedule).where(BackupSchedule.branch_id == branch.id)
        res = await db.execute(stmt)
        schedule = res.scalar_one_or_none()
        if schedule:
            return schedule

        stmt = select(BackupSchedule).where(
            BackupSchedule.organization_id == (await branch.awaitable_attrs.project).organization_id,
            BackupSchedule.env_type == branch.env_type,
            BackupSchedule.branch_id.is_(None),
        )
        res = await db.execute(stmt)
        schedule = res.scalar_one_or_none()
        if schedule:
            return schedule

        stmt = select(BackupSchedule).where(
            BackupSchedule.organization_id == (await branch.awaitable_attrs.project).organization_id,
            BackupSchedule.branch_id.is_(None),
            BackupSchedule.env_type.is_(None),
        )
        res = await db.execute(stmt)
        return res.scalar_one_or_none()

    async def execute_backup(self, db: AsyncSession, branch: Branch, row: BackupScheduleRow, nb: NextBackup):
        be = BackupEntry(branch_id=branch.id, row_index=row.row_index, created_at=datetime.utcnow(), size_bytes=0)
        db.add(be)
        await db.commit()
        await db.refresh(be)

        log_entry = BackupLog(branch_id=branch.id, backup_uuid=str(be.id), action="taken", ts=datetime.utcnow())
        db.add(log_entry)

        nb.next_at = nb.next_at + timedelta(seconds=interval_seconds(row.interval, row.unit))
        db.add(nb)
        await db.commit()

        await self.prune_backups(db, branch, row)

        logger.info("Backup created %s for branch %s row %d", be.id, branch.id, row.row_index)

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
        for b in to_delete:
            log = BackupLog(branch_id=branch.id, backup_uuid=str(b.id), action="delete", ts=datetime.utcnow())
            db.add(log)
            await db.delete(b)
        await db.commit()
        logger.info("Pruned %d old backups for branch %s row %d", len(to_delete), branch.id, row.row_index)


# in main.py or backupmonitor.py
async def run_monitor():
    monitor = BackupMonitor()
    while True:
        try:
            async with AsyncSessionLocal() as db:
                await monitor.run_once(db)
        except Exception:
            logger.exception("Error running backup monitor iteration")
        await asyncio.sleep(POLL_INTERVAL)
