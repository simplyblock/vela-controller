#!/usr/bin/env python3
"""
Improved Flask-based Backup Monitor for Kubernetes PVC snapshots.

Run: python backup_monitor_improved.py

Environment variables:
- DATABASE_URL
- K8S_NAMESPACE
- VOLUME_SNAPSHOT_CLASS
- POLL_INTERVAL
- SNAPSHOT_TIMEOUT_SEC
"""

import threading
import uuid
import logging
from datetime import timedelta
from fastapi import FastAPI
from api.Models.backups import *
# at the top of api/backupmonitor.py
from .Models.base import *  # adjust path if needed
from .Models.backups import *
from datetime import datetime

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
# Backup Monitor
# ---------------------------
class BackupMonitor:
    def __init__(self, session_factory):
        self.session_factory = session_factory
        self.branch_locks = {}

    def get_branch_lock(self, branch_id):
        if branch_id not in self.branch_locks:
            self.branch_locks[branch_id] = threading.Lock()
        return self.branch_locks[branch_id]

    def run_once(self):
        now = datetime.now()
        session = self.session_factory()
        try:
            branches = session.query(Branch).filter(Branch.status == "active").all()
            logger.info("Found %d online branches", len(branches))

            for branch in branches:
                try:
                    self.process_branch(branch, now)
                except Exception:
                    logger.exception("Error processing branch %s", getattr(branch, "id", branch.id))

            session.commit()
        except Exception:
            session.rollback()
            logger.exception("Session-level failure in run_once")
        finally:
            session.close()

    def process_branch(self, branch: Branch, now: datetime):
        """Ensure NextBackup rows exist and trigger backups for due entries"""
        session = self.session_factory()
        try:
            branch_local = session.query(Branch).filter(Branch.id == branch.id).one_or_none()
            if not branch_local:
                logger.debug("Branch %s disappeared", branch.id)
                return

            schedule = self.resolve_schedule(session, branch_local)
            if not schedule:
                logger.info("No schedule for branch %s", branch_local.id)
                return

            #logger.info("Processing branch %s schedule %s", branch_local.id, schedule.id)
            # Iterate ologger.debug("No schedule for branch %s", branch_local.id)ver all schedule rows
            # Ensure NextBackup entries exist for all schedule rows
            for row in sorted(schedule.rows, key=lambda r: r.row_index):
                nb = (
                    session.query(NextBackup)
                    .filter(NextBackup.branch_id == branch_local.id, NextBackup.row_index == row.row_index)
                    .one_or_none()
                )
                if nb is None:
                    # create all missing NextBackup entries
                    nb = NextBackup(
                        branch_id=branch_local.id,
                        schedule_id=schedule.id,
                        row_index=row.row_index,
                        next_at=now + timedelta(seconds=interval_seconds(row.interval, row.unit))
        # due immediately
                    )
                    session.add(nb)
            session.commit()

            logger.info("Created NextBackup for branch %s row %d", branch_local.id, row.row_index)

            # Iterate over all NextBackup rows
            next_backups = session.query(NextBackup).filter(NextBackup.branch_id == branch_local.id).all()

            for nb in next_backups:
                if nb.next_at <= now:
                    row = session.query(BackupScheduleRow).filter(
                        BackupScheduleRow.schedule_id == schedule.id,
                        BackupScheduleRow.row_index == nb.row_index
                    ).one_or_none()
                    if not row:
                        logger.debug("no row found")
                        continue

                    lock = self.get_branch_lock(branch_local.id)
                    if not lock.acquire(blocking=False):
                        logger.debug("Skipping snapshot for %s row %d because another worker holds lock",
                                     branch_local.id, nb.row_index)
                        continue
                    try:
                        logger.info("Taking simulated snapshot for branch %s row %d", branch_local.id, nb.row_index)

                        # id | branch_id | backup_uuid | row_index | created_at | size_bytes
                        #----+-----------+-------------+-----------+------------+------------
                        # Instead of real snapshot, just create BackupEntry
                        be = BackupEntry(
                            branch_id=branch_local.id,
                            row_index=nb.row_index,
                            created_at=datetime.now(),
                            size_bytes=None
                        )
                        session.add(be)
                        session.commit()

                        # log the action
                        log_entry = BackupLog(
                            branch_id=branch_local.id,
                            backup_uuid=be.id,
                            action="taken",
                            ts=datetime.now()
                        )
                        session.add(log_entry)
                        session.commit()

                        # advance next_at
                        nb.next_at = nb.next_at + timedelta(seconds=interval_seconds(row.interval, row.unit))
                        session.add(nb)

                        session.commit()

                        # prune backups according to retention
                        self.prune_backups(session, branch_local, row)

                    finally:
                        lock.release()
        finally:
            session.close()

    def resolve_schedule(self, session, branch):
        # branch-level
        schedule = session.query(BackupSchedule).filter_by(branch_id=branch.id).one_or_none()
        if schedule:
            return schedule
        # env-level
        schedule = session.query(BackupSchedule).filter(
            BackupSchedule.organization_id==branch.organization_id,
            BackupSchedule.env_type==branch.env_type,
            BackupSchedule.branch_id.is_(None)
        ).one_or_none()
        if schedule:
            return schedule
        # org-leve        BackupSchedule.env_type==None,l
        schedule = session.query(BackupSchedule).filter(
            BackupSchedule.organization_id==branch.organization_id,
            BackupSchedule.branch_id.is_(None),
            BackupSchedule.env_type.is_(None)
        ).one_or_none()
        return schedule

    def execute_backup(self, session, branch, row, nb):
        # Simulate backup creation
        be = BackupEntry(branch_id=branch.id, row_index=row.row_index)
        session.add(be)
        log = BackupLog(branch_id=branch.id, backup_uuid=be.id, action="create")
        session.add(log)

        # Advance next_at
        nb.next_at = nb.next_at + timedelta(seconds=interval_seconds(row.interval, row.unit))
        session.add(nb)
        session.commit()
        logger.info(f"Backup created {be.id} for branch {branch.id} row {row.row_index}")

        # Prune old backups beyond retention
        self.prune_backups(session, branch, row)

    def prune_backups(self, session, branch, row):
        backups = session.query(BackupEntry).filter_by(branch_id=branch.id, row_index=row.row_index).order_by(BackupEntry.created_at.asc()).all()
        if len(backups) <= row.retention:
            return
        to_delete = backups[:len(backups)-row.retention]
        for b in to_delete:
            log = BackupLog(branch_id=branch.id, backup_uuid=b.id, action="delete")
            session.add(log)
            session.delete(b)
            session.commit()
            logger.info(f"Backup deleted {b.id} for branch {branch.id} row {row.row_index}")


# ---------------------------
# Background worker
# ---------------------------
monitor = BackupMonitor(SessionFactory)
stop_event = threading.Event()




