from warnings import catch_warnings

import json

from .backupmonitor import *
from pydantic import ConfigDict
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from datetime import datetime
import uuid
from fastapi import Request

# Your SQLAlchemy models
# from models import Organization, Branch, BackupSchedule, BackupScheduleRow, BackupEntry, BackupLog
# from db import SessionFactory, get_db

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

# backups.py
from fastapi import APIRouter
router = APIRouter()


# ---------------------------
# Pydantic Schemas
# ---------------------------

# ---------------------------
# Health endpoint
# ---------------------------
@router.get("/healthz")
def healthz():
    return {"status": "ok"}


# ---------------------------
# Add or replace schedule
# ---------------------------
from fastapi import APIRouter, HTTPException, Depends, Body
from typing import List, Optional
from uuid import UUID
from sqlalchemy.orm import Session
from .db import get_db
from .Models.base import Organization, Branch, Project
from .Models.backups import BackupSchedule, BackupScheduleRow
import uuid

router = APIRouter()

# ---------------------------
# Add or replace schedule
# ---------------------------
@router.post("/backup/organizations/{org_ref}/schedule", response_model=None)
@router.put("/backup/organizations/{org_ref}/schedule", response_model=None)
@router.post("/backup/branches/{branch_ref}/schedule", response_model=None)
@router.put("/backup/branches/{branch_ref}/schedule", response_model=None)
async def add_or_replace_backup_schedule(
    payload: Request,
    org_ref: Optional[UUID] = None,
    branch_ref: Optional[UUID] = None,
    env_type: Optional[str] = None,
    db: Session = Depends(get_db),
    method: Optional[str] = None
):
    http_method = payload.method
    try:
       data = await payload.json()
       rows = data["rows"]
    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        print(f"JSON Decode Error: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON response format.")

    print(f"Rows data: {rows}")

    branch=None
    org=None
    branch_id=None
    org_id=None
    if org_ref:
       org = db.query(Organization).filter_by(id=org_ref).one_or_none()
       org_id = org.id
    elif branch_ref:
       branch = db.query(Branch).filter_by(id=branch_ref).one_or_none()
       branch_id = branch.id
    if not org and not branch:
        raise HTTPException(status_code=404, detail=f"Valid branch or organization required.")

    schedule = None

    schedule = db.query(BackupSchedule).filter_by(
              organization_id=org_id, env_type=env_type, branch_id=branch_id
        ).one_or_none()
    if http_method == "POST" and schedule:
            raise HTTPException(status_code=400, detail=f"Schedule already exists.")
    elif http_method == "PUT" and not schedule:
        raise HTTPException(status_code=404, detail="No schedule found.")

    if not schedule:
        schedule = BackupSchedule(
            organization_id=org_id,
            env_type=env_type,
            branch_id=branch_id
        )
        db.add(schedule)
        db.flush()

    total = 0
    processed_rows = []

    if len(rows) > 10:
        raise HTTPException(status_code=422, detail="Too many rows in schedule. Max: 10")

    for idx, r in enumerate(rows):
        if any(rs['interval'] == r["interval"] and rs['unit'] == r["unit"] for rs in rows[:idx]):
            raise HTTPException(status_code=422, detail="Duplicate row found in schedule")
        if r["unit"] not in VALID_UNITS:
            raise HTTPException(status_code=400, detail=f"Invalid unit: {r['unit']}")
        if r["interval"] > INTERVAL_LIMITS[r["unit"]]:
            raise HTTPException(status_code=400,
                                detail=f"Interval for {r['unit']} cannot exceed {INTERVAL_LIMITS[r['unit']]}")

        total += r["retention"]
        processed_rows.append(r)

        # Check max backups allowed for the organization or branch
    if branch is None and total > org.max_backups:
        raise HTTPException(status_code=422,
                            detail=f"Max Backups {org.max_backups} of Organization {org_ref} exceeded: {total}")
    elif branch and total > branch.max_backups:
        raise HTTPException(status_code=422,
                            detail=f"Max Backups {branch.max_backups} of Branch {branch_ref} exceeded: {total}")

        # Delete existing rows if PUT method and schedule exists
    if http_method == "PUT":
        rows_to_delete = db.query(BackupScheduleRow).filter_by(schedule_id=schedule.id).all()
        # Print the rows to stdout
        if rows_to_delete:
            print(f"Deleting the following rows for schedule {schedule.id}:")
            for row in rows_to_delete:
                print(
                    f"Row ID: {row.id}, Row Index: {row.row_index}, Retention: {row.retention}, Interval: {row.interval}, Unit: {row.unit}")
        db.query(BackupScheduleRow).filter_by(schedule_id=schedule.id).delete(synchronize_session=False)
        db.commit()

        # Add new rows to the schedule
    for r in processed_rows:
        row = BackupScheduleRow(
            schedule_id=schedule.id,
            row_index=r["row_index"],
            interval=r["interval"],
            unit=r["unit"],
            retention=r["retention"]
        )
        db.add(row)

        # Commit changes to DB
    db.commit()

    return {"status": "ok", "schedule_id": schedule.id}


# ---------------------------
# List schedules
# ---------------------------
@router.get(
    "/backup/organizations/{org_ref}/schedule",
)
@router.get(
    "/backup/branches/{branch_ref}/schedule",
)
def list_schedules(org_ref: Optional[UUID]=None, branch_ref: Optional[UUID] = None, env_type: Optional[str]=None, db: Session = Depends(get_db)):
    schedules=None
    schedules = db.query(BackupSchedule).filter(
        BackupSchedule.organization_id == org_ref,
        BackupSchedule.env_type == env_type,
        BackupSchedule.branch_id == branch_ref
    ).all()
    if not schedules:
        raise HTTPException(status_code=404, detail=f"no schedules found.")
    out = []
    for s in schedules:
        out.append({
            "ref": s.id,
            "organization_id": s.organization_id,
            "branch_id": s.branch_id,
            "env_type": s.env_type,
            "rows": [{"row_index": r.row_index, "interval": r.interval, "unit": r.unit, "retention": r.retention} for r in s.rows]
        })
    return out

# ---------------------------
# List backups
# ---------------------------
@router.get(
    "/backup/organizations/{org_ref}/",
)
@router.get(
    "/backup/branches/{branch_ref}/",
)
def list_backups(org_ref: Optional[UUID]=None, branch_ref: Optional[UUID] = None, env_type: Optional[str]=None, db: Session = Depends(get_db)):
    backups = None
    all_backups = []
    if org_ref:
       if env_type:
          branches = db.query(Branch).filter(Branch.organization_id==org_ref, Branch.env_type==env_type).all()
       else:
           branches = db.query(Branch).filter(Branch.organization_id==org_ref).all()
       for b in branches:
               # Query the BackupEntry table, filter by matching branch id, and accumulate the results
               backups_for_branch = db.query(BackupEntry).filter(BackupEntry.branch_id == b.id).all()
               # Add the backups for the current branch to the all_backups list
               all_backups.extend(backups_for_branch)
    elif branch_ref:
       print("branch ref",branch_ref)
       backups = db.query(BackupEntry).filter(BackupEntry.branch_id==branch_ref).all()
    if not backups and not len(all_backups):
       raise HTTPException(status_code=404, detail=f"no backups found.")
    if not backups:
       backups=all_backups

    out = [{
        "branch_id": b.branch.id,
        "backup_uuid": b.id,
        "row_index": b.row_index,
        "created_at": b.created_at.isoformat()
    } for b in backups]
    return out

# ---------------------------
# Delete schedule
# ---------------------------
@router.delete(
    "/backup/organizations/{org_ref}/schedule",
)
@router.delete(
    "/backup/branches/{branch_ref}/schedule",
)
def delete_schedule(
        org_ref: Optional[UUID] = None,
        branch_ref: Optional[UUID] = None,
        env_type: Optional[str] = None,
        db: Session = Depends(get_db)
):

    query = db.query(BackupSchedule)
    # Apply filters based on what is provided
    if org_ref:
        if env_type:
            query = query.filter_by(BackupSchedule.organization_id==org_ref, BackupSchedule.env_type==env_type)
        else:
            query = query.filter_by(BackupSchedule.organization_id==org_ref,BackupSchedule.env_type==None)
    elif branch_ref:
        query = query.filter_by(BackupSchedule.branch_id==branch_ref)
    else:
        raise HTTPException(status_code=400, detail="either org-ref or branch-ref needed.")
    # Fetch the schedule, it will return either one schedule or None
    schedule = query.one_or_none()

    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    # Delete related BackupScheduleRow and NextBackup entries
    db.query(BackupScheduleRow).filter_by(BackupScheduleRow.schedule_id==schedule.id).delete()
    db.query(NextBackup).filter_by(NextBackup.schedule_id==schedule.id).delete()

    # Delete the main schedule
    db.delete(schedule)

    # Commit the changes
    db.commit()

    # Return a success message
    return {"status": "success", "message": "Schedule and related data deleted successfully"}

    return {"status": "schedule deleted"}


# ---------------------------
# Manual backup
# ---------------------------
@router.post("/backup/branches/{branch_ref}/")
def manual_backup(branch_ref: UUID, db: Session = Depends(get_db)):
    branch = db.query(Branch).filter_by(id=branch_ref).first()
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")

    backup = BackupEntry(
        branch_id=branch.id,
        row_index=-1,
        created_at=datetime.now(),
        size_bytes=0
    )
    db.add(backup)
    db.commit()

    log = BackupLog(
        branch_id=branch.id,
        action="manual-create",
        ts=datetime.now(),
        backup_uuid=backup.id
    )
    db.add(log)
    db.commit()
    return {"status": "manual backup created", "backup_id": backup.id}


# ---------------------------
# Delete backup
# ---------------------------
@router.delete("/backup/{backup_ref}")
def delete_backup(backup_ref: UUID, db: Session = Depends(get_db)):
    backup = db.query(BackupEntry).filter(BackupEntry.id==backup_ref).first()
    if not backup:
        raise HTTPException(status_code=404, detail="Backup not found")

    db.delete(backup)
    db.commit()

    log = BackupLog(
        branch_id=backup.branch_id,
        backup_uuid=backup.id,
        action="manual-delete",
        ts=datetime.now()
    )
    db.add(log)
    db.commit()
    return {"status": "backup deleted"}

