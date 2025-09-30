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

import os
import threading
import signal
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from flask import Flask, jsonify, request
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    BigInteger,
    func,
    Index,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from fastapi import FastAPI, HTTPException, Depends


app = FastAPI()

# kubernetes client
from kubernetes import client as k8s_client, config as k8s_config
from kubernetes.client.rest import ApiException

from flask import Flask
app = Flask(__name__)


# ---------------------------
# Basic config & logging
# ---------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL"
)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

K8S_NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")
VOLUME_SNAPSHOT_CLASS = os.environ.get("VOLUME_SNAPSHOT_CLASS", "csi-snapshot-class")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("backup-monitor")

# ---------------------------
# DB models
# ---------------------------
Base = declarative_base()


class Organization(Base):
    __tablename__ = "organizations"
    id = Column(Integer, primary_key=True)
    ref = Column(String, unique=True, nullable=False)
    name = Column(String)
    max_backups = Column(Integer, nullable=False)

class Project(Base):
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True)
    ref = Column(String, unique=True, nullable=False)
    organization_id = Column(Integer, ForeignKey("organizations.id"), nullable=False)
    organization = relationship("Organization")

class Branch(Base):
    __tablename__ = "branches"
    id = Column(Integer, primary_key=True)
    ref = Column(String, unique=True, nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    project = relationship("Project")
    env_type = Column(String, nullable=False)
    is_online = Column(Boolean, default=True)
    max_backups = Column(Integer, nullable=False)

class BackupSchedule(Base):
    __tablename__ = "backup_schedules"
    id = Column(Integer, primary_key=True)
    ref = Column(String, unique=True, nullable=False)
    organization_id = Column(Integer, ForeignKey("organizations.id"), nullable=False)
    organization = relationship("Organization")
    env_type = Column(String, nullable=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=True)
    branch = relationship("Branch")
    rows = relationship(
        "BackupScheduleRow",
        back_populates="schedule",
        cascade="all, delete-orphan",
        single_parent=True,
    )


class BackupScheduleRow(Base):
    __tablename__ = "backup_schedule_rows"
    id = Column(Integer, primary_key=True)
    schedule_id = Column(Integer, ForeignKey("backup_schedules.id", ondelete="CASCADE"), nullable=False)
    schedule = relationship("BackupSchedule", back_populates="rows")
    row_index = Column(Integer, nullable=False)
    interval = Column(Integer, nullable=False)
    unit = Column(String, nullable=False)
    retention = Column(Integer, nullable=False)
    __table_args__ = (Index("ix_backup_schedule_row_schedule_index", "schedule_id", "row_index", unique=True),)

class NextBackup(Base):
    __tablename__ = "next_backups"
    id = Column(Integer, primary_key=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    branch = relationship("Branch")
    schedule_ref = Column(String, nullable=False)
    row_index = Column(Integer, nullable=False)
    next_at = Column(DateTime, nullable=False, index=True)
    __table_args__ = (Index("ix_next_backups_branch_row", "branch_id", "row_index", unique=True),)


# Add composite index suggestion: (branch_id, row_index) unique in production
Index("ix_next_backups_branch_row", NextBackup.branch_id, NextBackup.row_index, unique=True)


class BackupEntry(Base):
    __tablename__ = "backups"
    id = Column(Integer, primary_key=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=False)
    branch = relationship("Branch")
    backup_uuid = Column(String, nullable=False, unique=True)
    row_index = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    size_bytes = Column(Integer, nullable=True)


class BackupLog(Base):
    __tablename__ = "backup_logs"
    id = Column(Integer, primary_key=True)
    branch_ref = Column(String, nullable=False)
    backup_uuid = Column(String, nullable=False)
    action = Column(String, nullable=False)  # "create" / "delete"
    ts = Column(DateTime, nullable=False, default=datetime.utcnow)

# ---------------------------
# DB setup
# ---------------------------
# pool_pre_ping helps with stale connections in long-running processes
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionFactory = sessionmaker(bind=engine)
Session = scoped_session(SessionFactory)

Base.metadata.create_all(bind=engine)

# ---------------------------
# Kubernetes helper
# ---------------------------

# ---------------------------
# Utility
# ---------------------------
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
            branches = session.query(Branch).filter(Branch.is_online == True).all()
            logger.info("Found %d online branches", len(branches))

            for branch in branches:
                try:
                    self.process_branch(branch, now)
                except Exception:
                    logger.exception("Error processing branch %s", getattr(branch, "ref", branch.id))

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
                logger.debug("Branch %s disappeared", branch.ref)
                return

            schedule = self.resolve_schedule(session, branch_local)
            if not schedule:
                logger.debug("No schedule for branch %s", branch_local.ref)
                return

            # Iterate over all schedule rows
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
                        schedule_ref=schedule.ref,
                        row_index=row.row_index,
                        next_at=now + timedelta(seconds=interval_seconds(row.interval, row.unit))
        # due immediately
                    )
                    session.add(nb)
            session.commit()

            logger.info("Created NextBackup for branch %s row %d", branch_local.ref, row.row_index)

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
                                     branch_local.ref, nb.row_index)
                        continue
                    try:
                        logger.info("Taking simulated snapshot for branch %s row %d", branch_local.ref, nb.row_index)

                        # id | branch_id | backup_uuid | row_index | created_at | size_bytes
                        #----+-----------+-------------+-----------+------------+------------
                        # Instead of real snapshot, just create BackupEntry
                        backup_uuid = f"sim-{branch_local.ref}-{nb.row_index}-{uuid.uuid4().hex[:8]}"
                        be = BackupEntry(
                            branch_id=branch_local.id,
                            backup_uuid=backup_uuid,
                            row_index=nb.row_index,
                            created_at=datetime.now(),
                            size_bytes=None
                        )
                        session.add(be)
                        session.commit()

                        # log the action
                        log_entry = BackupLog(
                            branch_ref=branch_local.ref,
                            backup_uuid=backup_uuid,
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
            BackupSchedule.organization_id==branch.project.organization_id,
            BackupSchedule.env_type==branch.env_type,
            BackupSchedule.branch_id==None
        ).one_or_none()
        if schedule:
            return schedule
        # org-leve        BackupSchedule.env_type==None,l
        schedule = session.query(BackupSchedule).filter(
            BackupSchedule.organization_id==branch.project.organization_id,
            BackupSchedule.branch_id==None
        ).one_or_none()
        return schedule

    def execute_backup(self, session, branch, row, nb):
        # Simulate backup creation
        backup_uuid = f"sim-{branch.ref}-{row.row_index}-{uuid.uuid4().hex[:8]}"
        be = BackupEntry(branch_id=branch.id, backup_uuid=backup_uuid, row_index=row.row_index)
        session.add(be)
        log = BackupLog(branch_ref=branch.ref, backup_uuid=backup_uuid, action="create")
        session.add(log)

        # Advance next_at
        nb.next_at = nb.next_at + timedelta(seconds=interval_seconds(row.interval, row.unit))
        session.add(nb)
        session.commit()
        logger.info(f"Backup created {backup_uuid} for branch {branch.ref} row {row.row_index}")

        # Prune old backups beyond retention
        self.prune_backups(session, branch, row)

    def prune_backups(self, session, branch, row):
        backups = session.query(BackupEntry).filter_by(branch_id=branch.id, row_index=row.row_index).order_by(BackupEntry.created_at.asc()).all()
        if len(backups) <= row.retention:
            return
        to_delete = backups[:len(backups)-row.retention]
        for b in to_delete:
            log = BackupLog(branch_ref=branch.ref, backup_uuid=b.backup_uuid, action="delete")
            session.add(log)
            session.delete(b)
            session.commit()
            logger.info(f"Backup deleted {b.backup_uuid} for branch {branch.ref} row {row.row_index}")


# ---------------------------
# Background worker
# ---------------------------
monitor = BackupMonitor(SessionFactory)
stop_event = threading.Event()

def background_worker():
    while not stop_event.is_set():
        try:
            monitor.run_once()
        except Exception:
            logger.exception("Monitor failed")
        stop_event.wait(POLL_INTERVAL)


def _handle_sig(signum, frame):
    stop_event.set()


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})

# ---------------------------
# Flask REST API
# ---------------------------

@app.route(
    "/organizations/<org_ref>/schedules",
    methods=["POST", "PUT"]
)
@app.route(
    "/organizations/<org_ref>/branches/<branch_ref>/schedules",
    methods=["POST", "PUT"]
)
def add_or_replace_backup_schedule(org_ref,env_type=None,branch_ref=None):
    """
    POST: add a new schedule
    PUT: replace existing schedule for same branch/env/org
    Body JSON example:
    {
        "org_ref": "org1",
        "env_type": "staging",
        "branch_ref": "branch1",  # optional
        "rows": [
            {"row_index": 0, "interval": 1, "unit": "hour", "retention": 5}
        ]
    }
    """
    data = request.json
    #org_ref = data.get("org_ref")
    env_type = data.get("env_type")
    branch_ref = data.get("branch_ref")
    rows = data.get("rows", [])

    session = SessionFactory()
    try:
        org = session.query(Organization).filter_by(ref=org_ref).one_or_none()
        if not org:
            return jsonify({"error": f"Organization {org_ref} not found"}), 404

        branch = None
        branch_id = None
        if branch_ref:
            branch = session.query(Branch).filter_by(ref=branch_ref, project_id=org.id).one_or_none()
            branch_id = branch.id if branch else None

        # check if schedule exists
        schedule = session.query(BackupSchedule).filter_by(
            organization_id=org.id, env_type=env_type, branch_id=branch_id
        ).one_or_none()

        if request.method == "POST" and schedule:
            return jsonify({"error": "Schedule already exists"}), 400

        if not schedule:
            schedule = BackupSchedule(
                ref=f"sched-{org_ref}-{env_type}-{branch_ref or 'none'}-{uuid.uuid4().hex[:6]}",
                organization_id=org.id,
                env_type=env_type,
                branch_id=branch_id
            )
            session.add(schedule)
            session.flush()

        schedule = session.query(Organization).filter(
            BackupSchedule.organization_id == branch.project.organization_id,
            BackupSchedule.env_type == branch.env_type,
            BackupSchedule.branch_id == None
        ).one_or_none()

        total=0

        if len(rows)>10:
            return jsonify({"error": f"Too many rows in schedule. Max: 10"}), 422

        processed_rows=[]
        for r in rows:

            idx = rows.index(r)
            subset = rows[:idx]
            is_duplicate = any(rs["interval"] == r["interval"] and rs["unit"] == r["unit"] for rs in subset)
            if is_duplicate:
                return jsonify({"error": f"Duplicate row found in schedule. Max: 10"}), 422

            processed_rows.update(r)

            total=r["retention"]+total
            if r["unit"] not in VALID_UNITS:
                status_code = 400
                detail = f"Invalid unit: {r["unit"]}"
                return jsonify({"error": detail}), status_code
            # validate interval limits
            if r["interval"] > INTERVAL_LIMITS[r["unit"]]:
                status_code=400
                detail=f"Interval for {r["unit"]} cannot exceed {INTERVAL_LIMITS[r["unit"]]}"
                return jsonify({"error": detail}), status_code

        if (branch == None):
            if (total > org.max_backups):
                return jsonify({"error": f"Max Backups {org.max_backups} of Organization {org_ref} exceeded: {total}"}), 422
        else:
            if (total > branch.max_backups):
                return jsonify({"error": f"Max Backups {org.max_backups} of Branch {branch_ref} exceeded: {total}"}), 422

        if request.method == "PUT" and schedule:
            for r in schedule.rows:
                session.delete(r)

        row = BackupScheduleRow(
                schedule_id=schedule.id,
                row_index=r["row_index"],
                interval=r["interval"],
                unit=r["unit"],
                retention=r["retention"]
        )
        session.add(row)

        session.commit()
        return jsonify({"status": "ok", "schedule_ref": schedule.ref})
    finally:
        session.close()

@app.route(
    "/organizations/<org_ref>/schedules",
    methods=["GET"]
)
@app.route(
    "/organizations/<org_ref>/branches/<branch_ref>/schedules",
    methods=["GET"]
)
def list_schedules():
    session = SessionFactory()
    try:
        schedules = session.query(BackupSchedule).all()
        out = []
        for s in schedules:
            out.append({
                "ref": s.ref,
                "organization_id": s.organization_id,
                "branch_id": s.branch_id,
                "env_type": s.env_type,
                "rows": [{"row_index": r.row_index, "interval": r.interval, "unit": r.unit, "retention": r.retention} for r in s.rows]
            })
        return jsonify(out)
    finally:
        session.close()

@app.route(
    "/organizations/<org_ref>/backups",
    methods=["GET"]
)
@app.route(
    "/organizations/<org_ref>/branches/<branch_ref>/backups",
    methods=["GET"]
)
def list_backups():
    session = SessionFactory()
    try:
        backups = session.query(BackupEntry).all()
        out = [{
            "branch_ref": b.branch.ref,
            "backup_uuid": b.backup_uuid,
            "row_index": b.row_index,
            "created_at": b.created_at.isoformat()
        } for b in backups]
        return jsonify(out)
    finally:
        session.close()


@app.route(
    "/organizations/<org_ref>/schedules",
    methods=["DELETE"]
)
@app.route(
    "/organizations/<org_ref>/branches/<branch_ref>/schedules",
    methods=["DELETE"]
)
def delete_schedule(schedule_id: int, db: Session = Depends(get_db)):
    schedule = db.query(BackupSchedule).filter_by(id=schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    # Remove rows + next_backups but keep historical backups intact
    db.query(BackupScheduleRow).filter_by(schedule_id=schedule_id).delete()
    db.query(NextBackup).filter_by(schedule_ref=schedule.ref).delete()
    db.delete(schedule)
    db.commit()
    return {"status": "schedule deleted"}


@app.route(
    "/organizations/<org_ref>/branches/<branch_ref>/backup",
    methods=["POST"]
)
def manual_backup(branch_id: int, db: Session = Depends(get_db)):
    branch = db.query(Branch).filter_by(id=branch_id).first()
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")

    backup = BackupEntry(
        branch_id=branch.id,
        backup_uuid=str(uuid.uuid4()),
        row_index=-1,
        created_at=datetime.now(),
        size_bytes=0
    )
    db.add(backup)
    db.commit()

    log = BackupLog(
        branch_id=branch.id,
        action="manual-create",
        created_at=datetime.now(),
        details=f"Manual backup for branch {branch.ref}"
    )
    db.add(log)
    db.commit()
    return {"status": "manual backup created", "backup_id": backup.id}


@app.route(
    "/organizations/<org_ref>/branches/<branch_ref>/backup",
    methods=["DELETE"]
)
def delete_backup(backup_id: int, db: Session = Depends(get_db)):
    backup = db.query(BackupEntry).filter_by(id=backup_id).first()
    if not backup:
        raise HTTPException(status_code=404, detail="Backup not found")

    db.delete(backup)
    db.commit()

    log = BackupLog(
        branch_id=backup.branch_id,
        action="manual-delete",
        created_at=datetime.now(),
        details=f"Deleted backup {backup.backup_uuid}"
    )
    db.add(log)
    db.commit()
    return {"status": "backup deleted"}


# ---------------------------
# Run Flask + background worker
# ---------------------------
if __name__ == "__main__":
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    # Start background worker thread
    t = threading.Thread(target=background_worker, daemon=True)
    t.start()

    # Start Flask REST server
    app.run(host="0.0.0.0", port=8080)

