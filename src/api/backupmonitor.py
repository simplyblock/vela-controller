# backup_monitor_async.py

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict

from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, delete

from .models.backups import (
    Branch,
    BackupSchedule,
    BackupScheduleRow,
    BackupEntry,
    BackupLog,
    NextBackup,
)
from .db import get_db  # async session generator

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
logger = logging.getLogger("backup-monitor")

UNIT_MULTIPLIER = {
    "min": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
    "w": 604800, "week": 604800, "weeks": 604800,
}

VALID_UNITS = {"minute", "hour", "day", "week", "min","minutes","h","hours","d","days","w","weeks"}
INTERVAL_LIMITS = {
    "minute": 59,"minutes": 59,"min": 59,
    "hour": 23,"hours": 23,"h": 23,
    "day": 6,"d": 6,"days": 6,
    "week": 12,"w": 12,"weeks": 12,
}


def interval_seconds(interval: int, unit: str) -> int:
    return interval * UNIT_MULTIPLIER[unit.lower()]


# ---------------------------
# Async Backup Monitor
# ---------------------------
class BackupMonitor:
    def __init__(self):
        # Each branch gets its own asyncio.Lock
        self.branch_locks: Dict[str, asyncio.Lock] = {}

    def get_branch_lock(self, branch_id: str) -> asyncio.Lock:
        if branch_id not in self.branch_locks:
            self.branch_locks[branch_id] = asyncio.Lock()
        return self.branch_locks[branch_id]

    async def run_once(self, db: AsyncSession):
        """Run a single iteration of backup monitoring."""
        now = datetime.utcnow()
        result = await db.exec(select(Branch))
        branches = result.all()
        logger.info("Found %d active branches", len(branches))

        for branch in branches:
            try:
                await self.process_branch(db, branch, now)
            except Exception:
                logger.exception("Error processing branch %s", branch.id)

    async def process_branch(self, db: AsyncSession, branch: Branch, now: datetime):
        """Ensure NextBackup rows exist and trigger due backups"""
        branch_local = await db.get(Branch, branch.id)
        if not branch_local:
            logger.debug("Branch %s disappeared", branch.id)
            return

        schedule = await self.resolve_schedule(db, branch_local)
        if not schedule:
            logger.info("No schedule for branch %s", branch_local.id)
            return

        # Ensure NextBackup exists for all schedule rows
        for row in sorted(schedule.rows, key=lambda r: r.row_index):
            stmt = select(NextBackup).where(
                NextBackup.branch_id == branch_local.id,
                NextBackup.row_index == row.row_index
            )
            res = await db.exec(stmt)
            nb = res.one_or_none()
            if nb is None:
                nb = NextBackup(
                    branch_id=branch_local.id,
                    schedule_id=schedule.id,
                    row_index=row.row_index,
                    next_at=now + timedelta(seconds=interval_seconds(row.interval, row.unit))
                )
                db.add(nb)
        await db.commit()

        # Process due NextBackup entries
        stmt = select(NextBackup).where(NextBackup.branch_id == branch_local.id)
        result = await db.exec(stmt)
        next_backups = result.all()

        for nb in next_backups:
            if nb.next_at <= now:
                stmt_row = select(BackupScheduleRow).where(
                    BackupScheduleRow.schedule_id == schedule.id,
                    BackupScheduleRow.row_index == nb.row_index
                )
                row_res = await db.exec(stmt_row)
                row = row_res.one_or_none()
                if not row:
                    continue

                lock = self.get_branch_lock(branch_local.id)
                if not lock.locked():
                    async with lock:
                        await self.execute_backup(db, branch_local, row, nb)
                else:
                    logger.debug(
                        "Skipping snapshot for %s row %d because another worker holds lock",
                        branch_local.id, nb.row_index
                    )

    async def resolve_schedule(self, db: AsyncSession, branch: Branch) -> BackupSchedule:
        """Resolve the applicable schedule for a branch"""
        # Branch-level
        stmt = select(BackupSchedule).where(BackupSchedule.branch_id == branch.id)
        res = await db.exec(stmt)
        schedule = res.one_or_none()
        if schedule:
            return schedule

        # Env-level
        stmt = select(BackupSchedule).where(
            BackupSchedule.organization_id == branch.organization_id,
            BackupSchedule.env_type == branch.env_type,
            BackupSchedule.branch_id.is_(None)
        )
        res = await db.exec(stmt)
        schedule = res.one_or_none()
        if schedule:
            return schedule

        # Org-level
        stmt = select(BackupSchedule).where(
            BackupSchedule.organization_id == branch.organization_id,
            BackupSchedule.branch_id.is_(None),
            BackupSchedule.env_type.is_(None)
        )
        res = await db.exec(stmt)
        return res.one_or_none()

    async def execute_backup(self, db: AsyncSession, branch: Branch, row: BackupScheduleRow, nb: NextBackup):
        """Simulate backup creation and advance next_at"""
        be = BackupEntry(
            branch_id=branch.id,
            row_index=row.row_index,
            created_at=datetime.utcnow(),
            size_bytes=None
        )
        db.add(be)
        await db.commit()
        await db.refresh(be)

        log_entry = BackupLog(
            branch_id=branch.id,
            backup_uuid=be.id,
            action="taken",
            ts=datetime.utcnow()
        )
        db.add(log_entry)

        # Advance next_at
        nb.next_at = nb.next_at + timedelta(seconds=interval_seconds(row.interval, row.unit))
        db.add(nb)
        await db.commit()

        await self.prune_backups(db, branch, row)

        logger.info("Backup created %s for branch %s row %d", be.id, branch.id, row.row_index)

    async def prune_backups(self, db: AsyncSession, branch: Branch, row: BackupScheduleRow):
        stmt = select(BackupEntry).where(
            BackupEntry.branch_id == branch.id,
            BackupEntry.row_index == row.row_index
        ).order_by(BackupEntry.created_at.asc())
        result = await db.exec(stmt)
        backups = result.all()
        if len(backups) <= row.retention:
            return
        to_delete = backups[:len(backups) - row.retention]
        for b in to_delete:
            log = BackupLog(
                branch_id=branch.id,
                backup_uuid=b.id,
                action="delete",
                ts=datetime.utcnow()
            )
            db.add(log)
            await db.delete(b)
        await db.commit()
        logger.info("Pruned %d old backups for branch %s row %d", len(to_delete), branch.id, row.row_index)


# ---------------------------
# Async background runner
# ---------------------------
async def run_monitor():
    monitor = BackupMonitor()
    while True:
        async for db in get_db():
            try:
                await monitor.run_once(db)
            except Exception:
                logger.exception("Error running backup monitor iteration")
        await asyncio.sleep(POLL_INTERVAL)


