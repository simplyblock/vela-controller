from pydantic import BaseModel
from enum import Enum

from sqlalchemy import func


class ResourceType(str, Enum):
    vcpu = "vcpu"
    ram = "ram"
    iops = "iops"
    backup_storage = "backup_storage"
    nvme = "nvme"

class ResourceLimitCreate(BaseModel):
    resource: ResourceType
    max_total: int
    max_per_branch: int

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Optional
from datetime import datetime
import threading, time

from .db import SessionLocal

from .Models.base import (Organization, Branch, Project)

from .Models.ressources import (
    BranchProvisioning, ResourceLimit,
    ResourceUsageMinute, ProvisioningLog,
    ResourceType, EntityType, ResourceConsumptionLimit
)

from fastapi import APIRouter
router = APIRouter()

# ---------------------------
# Dependency
# ---------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------
# Helper functions
# ---------------------------

from sqlalchemy import func

def get_effective_branch_limits(db: Session, branch_id: str) -> dict:
    """
    Compute effective provisioning limits for all resource types for a branch.
    Returns a dict: {resource_type: effective_limit, ...}
    """
    branch = db.query(Branch).filter_by(id=branch_id).first()
    if not branch:
        raise HTTPException(404, f"Branch {branch_id} not found")
    project = branch.project
    org = project.org

    effective_limits = {}

    for resource in ResourceType:
        # --- Get limits ---
        org_limit = db.query(ResourceLimit).filter_by(
            entity_type=EntityType.org,
            org_id=org.id,
            project_id=None,
            resource=resource
        ).first()

        project_limit = db.query(ResourceLimit).filter_by(
            entity_type=EntityType.project,
            org_id=org.id,
            project_id=project.id,
            resource=resource
        ).first()

        # --- Determine per-branch cap ---
        per_branch_limit = None
        if project_limit and project_limit.max_per_branch is not None:
            per_branch_limit = project_limit.max_per_branch
        elif org_limit and org_limit.max_per_branch is not None:
            per_branch_limit = org_limit.max_per_branch
        else:
            # If no per-branch limit defined, skip this resource
            per_branch_limit = 32000

        # --- Aggregate usage ---
        org_provisioned = (
            db.query(func.sum(BranchProvisioning.amount))
            .join(Branch, BranchProvisioning.branch_id == Branch.id)
            .join(Project, Branch.project_id == Project.id)
            .filter(Project.org_id == org.id, BranchProvisioning.resource == resource)
            .scalar()
        ) or 0

        project_provisioned = (
            db.query(func.sum(BranchProvisioning.amount))
            .join(Branch, BranchProvisioning.branch_id == Branch.id)
            .filter(Branch.project_id == project.id, BranchProvisioning.resource == resource)
            .scalar()
        ) or 0

        # --- Remaining capacity ---
        remaining_org = (org_limit.max_total - org_provisioned) if org_limit else float("inf")
        remaining_project = (project_limit.max_total - project_provisioned) if project_limit else float("inf")

        # --- Effective limit ---
        effective_limits[resource.value] = max(min(per_branch_limit, remaining_org, remaining_project), 0)

    return effective_limits



def get_total_allocated(db: Session, project_id: str, resource: ResourceType):
    """
    Sum of allocated resources in a project
    """
    total = db.query(BranchProvisioning).join(Branch).filter(
        Branch.project_id == project_id,
        BranchProvisioning.resource == resource
    ).with_entities(func.coalesce(func.sum(BranchProvisioning.amount),0)).scalar()
    return total

def log_provisioning(db: Session, branch_id: str, resource: ResourceType, amount: int, action: str, reason: Optional[str] = None):
    log = ProvisioningLog(
        branch_id=branch_id,
        resource=resource,
        amount=amount,
        action=action,
        reason=reason,
        ts=datetime.utcnow()
    )
    db.add(log)
    db.commit()


# ---------------------------
# Provisioning endpoints
# ---------------------------
@router.post("/branches/{branch_id}/provision")
def provision_branch(branch_id: str, provision: Dict[ResourceType, int], db: Session = Depends(get_db)):
    branch = db.query(Branch).filter_by(id=branch_id).first()
    if not branch:
        raise HTTPException(404, "Branch not found")

    project = branch.project
    org = project.org

    effective_limits=get_effective_branch_limits(db,branch_id)

    for rtype, amount in provision.items():
        # Check effective branch/project/org limits
        effective_limit = effective_limits[rtype]
        if effective_limit is not None and amount > effective_limit:
            raise HTTPException(422, f"{rtype} limit exceeded for branch {branch.id}")

        # Check total allocation in project/org
        total_allocated = get_total_allocated(db, project.id, rtype)
        if effective_limit is not None and (total_allocated + amount) > effective_limit:
            raise HTTPException(422, f"Total allocation for {rtype} exceeds project/org limit")

        # Create or update provisioning
        bp = db.query(BranchProvisioning).filter_by(branch_id=branch_id, resource=rtype).first()
        if bp:
            bp.amount = amount
        else:
            bp = BranchProvisioning(branch_id=branch_id, resource=rtype, amount=amount)
            db.add(bp)
        db.commit()
        log_provisioning(db, branch_id, rtype, amount, "create")

    return {"status": "ok"}


@router.get("/branches/{branch_id}/provision")
def get_branch_provisioning_api(branch_id: str, db: Session = Depends(get_db)):
    provisionings = db.query(BranchProvisioning).filter_by(branch_id=branch_id).all()
    return {p.resource.value: p.amount for p in provisionings}


# ---------------------------
# Resource usage endpoints
# ---------------------------
@router.get("/projects/{project_id}/usage")
def get_project_usage(project_id: str, cycle_start: Optional[datetime] = None,
                      cycle_end: Optional[datetime] = None, db: Session = Depends(get_db)):
    query = db.query(ResourceUsageMinute).filter_by(project_id=project_id)
    if cycle_start:
        query = query.filter(ResourceUsageMinute.ts_minute >= cycle_start)
    if cycle_end:
        query = query.filter(ResourceUsageMinute.ts_minute < cycle_end)

    usages = query.all()
    result: Dict[str, int] = {}
    for u in usages:
        result.setdefault(u.resource.value, 0)
        result[u.resource.value] += u.amount
    return result

@router.get("/organizations/{org_id}/usage")
def get_org_usage(org_id: str, cycle_start: Optional[datetime] = None,
                  cycle_end: Optional[datetime] = None, db: Session = Depends(get_db)):
    query = db.query(ResourceUsageMinute).filter_by(org_id=org_id)
    if cycle_start:
        query = query.filter(ResourceUsageMinute.ts_minute >= cycle_start)
    if cycle_end:
        query = query.filter(ResourceUsageMinute.ts_minute < cycle_end)

    usages = query.all()
    result: Dict[str, int] = {}
    for u in usages:
        result.setdefault(u.resource.value, 0)
        result[u.resource.value] += u.amount
    return result

@router.post("/limits/provisioning/{entity_type}/{entity_id}")
def set_provisioning_limit(
    entity_type: EntityType,
    entity_id: str,
    resource: ResourceType,
    max_total: int,
    max_per_branch: int,
    db: Session = Depends(get_db),
):
    if entity_type == EntityType.org:
        org_id, project_id = entity_id, None
    elif entity_type == EntityType.project:
        project = db.query(Project).filter_by(id=entity_id).first()
        if not project:
            raise HTTPException(404, "Project not found")
        org_id, project_id = project.org_id, project.id
    else:
        raise HTTPException(400, "Unsupported entity type")

    limit = db.query(ResourceLimit).filter_by(
        entity_type=entity_type,
        org_id=org_id,
        project_id=project_id,
        resource=resource
    ).first()

    if limit:
        limit.max_total = max_total
        limit.max_per_branch = max_per_branch
    else:
        limit = ResourceLimit(
            entity_type=entity_type,
            org_id=org_id,
            project_id=project_id,
            resource=resource,
            max_total=max_total,
            max_per_branch=max_per_branch
        )
        db.add(limit)
    db.commit()
    return {"status": "ok", "limit": limit.id}


@router.get("/limits/provisioning/{entity_type}/{entity_id}")
def get_provisioning_limits(
    entity_type: EntityType,
    entity_id: str,
    db: Session = Depends(get_db)
):
    q = db.query(ResourceLimit).filter_by(entity_type=entity_type)
    if entity_type == EntityType.org:
        q = q.filter_by(org_id=entity_id)
    elif entity_type == EntityType.project:
        q = q.filter_by(project_id=entity_id)
    return [dict(
        resource=l.resource.value,
        max_total=l.max_total,
        max_per_branch=l.max_per_branch
    ) for l in q.all()]

@router.post("/limits/consumption/{entity_type}/{entity_id}")
def set_consumption_limit(
    entity_type: EntityType,
    entity_id: str,
    resource: ResourceType,
    max_total_minutes: int,
    db: Session = Depends(get_db),
):
    if entity_type == EntityType.org:
        org_id, project_id = entity_id, None
    elif entity_type == EntityType.project:
        project = db.query(Project).filter_by(id=entity_id).first()
        if not project:
            raise HTTPException(404, "Project not found")
        org_id, project_id = project.org_id, project.id
    else:
        raise HTTPException(400, "Unsupported entity type")

    limit = db.query(ResourceConsumptionLimit).filter_by(
        entity_type=entity_type,
        org_id=org_id,
        project_id=project_id,
        resource=resource
    ).first()

    if limit:
        limit.max_total_minutes = max_total_minutes
    else:
        limit = ResourceConsumptionLimit(
            entity_type=entity_type,
            org_id=org_id,
            project_id=project_id,
            resource=resource,
            max_total_minutes=max_total_minutes
        )
        db.add(limit)
    db.commit()
    return {"status": "ok", "limit": limit.id}


@router.get("/limits/consumption/{entity_type}/{entity_id}")
def get_consumption_limits(
    entity_type: EntityType,
    entity_id: str,
    db: Session = Depends(get_db)
):
    q = db.query(ResourceConsumptionLimit).filter_by(entity_type=entity_type)
    if entity_type == EntityType.org:
        q = q.filter_by(org_id=entity_id)
    elif entity_type == EntityType.project:
        q = q.filter_by(project_id=entity_id)
    return [dict(
        resource=l.resource.value,
        max_total_minutes=l.max_total_minutes
    ) for l in q.all()]

# ---------------------------
# Worker for per-minute metering
# ---------------------------
def monitor_resources():
    while True:
        session = SessionLocal()
        ts_minute = datetime.utcnow().replace(second=0, microsecond=0)
        branches = session.query(Branch).filter(Branch.status == "online").all()
        for b in branches:
            provisionings = session.query(BranchProvisioning).filter_by(branch_id=b.id).all()
            for p in provisionings:
                usage = ResourceUsageMinute(
                    ts_minute=ts_minute,
                    org_id=b.project.org_id,
                    project_id=b.project_id,
                    branch_id=b.id,
                    resource=p.resource,
                    amount=p.amount
                )
                session.add(usage)
        session.commit()
        session.close()
        time.sleep(60)

@router.get("/branches/{branch_id}/limits/{resource}")
def branch_effective_limit(
    branch_id: str,
    resource: ResourceType,
    db: Session = Depends(get_db)
):
    limit = get_effective_branch_limit(db, branch_id, resource)
    return {"branch_id": branch_id, "resource": resource.value, "effective_limit": limit}



# Start worker thread
threading.Thread(target=monitor_resources, daemon=True).start()




