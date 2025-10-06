
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from typing import Dict, Optional
from datetime import datetime

from .db import _get_session
from .models.base import Organization, Project, Branch
from .models.ressources import (
    BranchProvisioning, ResourceLimit,
    ResourceUsageMinute, ProvisioningLog,
    ResourceType, EntityType, ResourceConsumptionLimit
)

router = APIRouter()


# ---------------------------
# Dependency
# ---------------------------
async def get_db():
    async for session in _get_session():
        yield session


# ---------------------------
# Helper functions
# ---------------------------
async def get_effective_branch_limits(db: AsyncSession, branch_id: str) -> dict:
    result = await db.exec(select(Branch).where(Branch.id == branch_id))
    branch = result.first()
    if not branch:
        raise HTTPException(404, f"Branch {branch_id} not found")

    project = branch.project
    org = project.organization

    effective_limits = {}

    for resource in ResourceType:
        org_limit = (await db.exec(
            select(ResourceLimit).where(
                ResourceLimit.entity_type == EntityType.org,
                ResourceLimit.org_id == org.id,
                ResourceLimit.project_id.is_(None),
                ResourceLimit.resource == resource
            )
        )).first()

        project_limit = (await db.exec(
            select(ResourceLimit).where(
                ResourceLimit.entity_type == EntityType.project,
                ResourceLimit.org_id == org.id,
                ResourceLimit.project_id == project.id,
                ResourceLimit.resource == resource
            )
        )).first()

        per_branch_limit = (
            project_limit.max_per_branch if project_limit and project_limit.max_per_branch is not None else
            org_limit.max_per_branch if org_limit and org_limit.max_per_branch is not None else 32000
        )

        # Aggregate usage
        org_prov = (await db.exec(
            select(func.sum(BranchProvisioning.amount)).join(Branch).join(Project)
            .where(Project.org_id == org.id, BranchProvisioning.resource == resource)
        )).one()[0] or 0

        proj_prov = (await db.exec(
            select(func.sum(BranchProvisioning.amount)).join(Branch)
            .where(Branch.project_id == project.id, BranchProvisioning.resource == resource)
        )).one()[0] or 0

        remaining_org = (org_limit.max_total - org_prov) if org_limit else float("inf")
        remaining_project = (project_limit.max_total - proj_prov) if project_limit else float("inf")

        effective_limits[resource.value] = max(min(per_branch_limit, remaining_org, remaining_project), 0)

    return effective_limits


async def get_total_allocated(db: AsyncSession, project_id: str, resource: ResourceType) -> int:
    total = await db.exec(
        select(func.coalesce(func.sum(BranchProvisioning.amount), 0))
        .join(Branch)
        .where(Branch.project_id == project_id, BranchProvisioning.resource == resource)
    )
    return total.one()[0] or 0


async def log_provisioning(
    db: AsyncSession,
    branch_id: str,
    resource: ResourceType,
    amount: int,
    action: str,
    reason: Optional[str] = None
):
    log = ProvisioningLog(
        branch_id=branch_id,
        resource=resource,
        amount=amount,
        action=action,
        reason=reason,
        ts=datetime.utcnow()
    )
    db.add(log)
    await db.commit()


# ---------------------------
# Provisioning endpoints
# ---------------------------
@router.post("/branches/{branch_id}/provision")
async def provision_branch(
    branch_id: str,
    provision: Dict[ResourceType, int],
    db: AsyncSession = Depends(get_db)
):
    result = await db.exec(select(Branch).where(Branch.id == branch_id))
    branch = result.first()
    if not branch:
        raise HTTPException(404, "Branch not found")

    effective_limits = await get_effective_branch_limits(db, branch_id)

    for rtype, amount in provision.items():
        effective_limit = effective_limits.get(rtype.value)
        if effective_limit is not None and amount > effective_limit:
            raise HTTPException(422, f"{rtype.value} limit exceeded for branch {branch.id}")

        total_allocated = await get_total_allocated(db, branch.project_id, rtype)
        if effective_limit is not None and (total_allocated + amount) > effective_limit:
            raise HTTPException(422, f"Total allocation for {rtype.value} exceeds project/org limit")

        # Create or update provisioning
        result = await db.exec(select(BranchProvisioning)
                               .where(BranchProvisioning.branch_id == branch_id,
                                      BranchProvisioning.resource == rtype))
        bp = result.first()
        if bp:
            bp.amount = amount
        else:
            bp = BranchProvisioning(branch_id=branch_id, resource=rtype, amount=amount)
            db.add(bp)

        await db.commit()
        await log_provisioning(db, branch_id, rtype, amount, "create")

    return {"status": "ok"}


@router.get("/branches/{branch_id}/provision")
async def get_branch_provisioning_api(branch_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.exec(select(BranchProvisioning).where(BranchProvisioning.branch_id == branch_id))
    provisionings = result.all()
    return {p.resource.value: p.amount for p in provisionings}


# ---------------------------
# Resource usage endpoints
# ---------------------------
@router.get("/projects/{project_id}/usage")
async def get_project_usage(
    project_id: str,
    cycle_start: Optional[datetime] = None,
    cycle_end: Optional[datetime] = None,
    db: AsyncSession = Depends(get_db)
):
    query = select(ResourceUsageMinute).where(ResourceUsageMinute.project_id == project_id)
    if cycle_start:
        query = query.where(ResourceUsageMinute.ts_minute >= cycle_start)
    if cycle_end:
        query = query.where(ResourceUsageMinute.ts_minute < cycle_end)

    result = await db.exec(query)
    usages = result.all()

    result_dict: Dict[str, int] = {}
    for u in usages:
        result_dict.setdefault(u.resource.value, 0)
        result_dict[u.resource.value] += u.amount
    return result_dict


@router.get("/organizations/{org_id}/usage")
async def get_org_usage(
    org_id: str,
    cycle_start: Optional[datetime] = None,
    cycle_end: Optional[datetime] = None,
    db: AsyncSession = Depends(get_db)
):
    query = select(ResourceUsageMinute).where(ResourceUsageMinute.org_id == org_id)
    if cycle_start:
        query = query.where(ResourceUsageMinute.ts_minute >= cycle_start)
    if cycle_end:
        query = query.where(ResourceUsageMinute.ts_minute < cycle_end)

    result = await db.exec(query)
    usages = result.all()

    result_dict: Dict[str, int] = {}
    for u in usages:
        result_dict.setdefault(u.resource.value, 0)
        result_dict[u.resource.value] += u.amount
    return result_dict


# ---------------------------
# Limits endpoints
# ---------------------------
@router.post("/limits/provisioning/{entity_type}/{entity_id}")
async def set_provisioning_limit(
    entity_type: EntityType,
    entity_id: str,
    resource: ResourceType,
    max_total: int,
    max_per_branch: int,
    db: AsyncSession = Depends(get_db)
):
    if entity_type == EntityType.org:
        org_id, project_id = entity_id, None
    elif entity_type == EntityType.project:
        result = await db.exec(select(Project).where(Project.id == entity_id))
        project = result.first()
        if not project:
            raise HTTPException(404, "Project not found")
        org_id, project_id = project.org_id, project.id
    else:
        raise HTTPException(400, "Unsupported entity type")

    result = await db.exec(select(ResourceLimit).where(
        ResourceLimit.entity_type == entity_type,
        ResourceLimit.org_id == org_id,
        ResourceLimit.project_id == project_id,
        ResourceLimit.resource == resource
    ))
    limit = result.first()

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

    await db.commit()
    return {"status": "ok", "limit": str(limit.id)}


@router.get("/limits/provisioning/{entity_type}/{entity_id}")
async def get_provisioning_limits(
    entity_type: EntityType,
    entity_id: str,
    db: AsyncSession = Depends(get_db)
):
    q = select(ResourceLimit).where(ResourceLimit.entity_type == entity_type)
    if entity_type == EntityType.org:
        q = q.where(ResourceLimit.org_id == entity_id)
    elif entity_type == EntityType.project:
        q = q.where(ResourceLimit.project_id == entity_id)

    result = await db.exec(q)
    return [dict(
        resource=l.resource.value,
        max_total=l.max_total,
        max_per_branch=l.max_per_branch
    ) for l in result.all()]


@router.post("/limits/consumption/{entity_type}/{entity_id}")
async def set_consumption_limit(
    entity_type: EntityType,
    entity_id: str,
    resource: ResourceType,
    max_total_minutes: int,
    db: AsyncSession = Depends(get_db)
):
    if entity_type == EntityType.org:
        org_id, project_id = entity_id, None
    elif entity_type == EntityType.project:
        result = await db.exec(select(Project).where(Project.id == entity_id))
        project = result.first()
        if not project:
            raise HTTPException(404, "Project not found")
        org_id, project_id = project.org_id, project.id
    else:
        raise HTTPException(400, "Unsupported entity type")

    result = await db.exec(select(ResourceConsumptionLimit).where(
        ResourceConsumptionLimit.entity_type == entity_type,
        ResourceConsumptionLimit.org_id == org_id,
        ResourceConsumptionLimit.project_id == project_id,
        ResourceConsumptionLimit.resource == resource
    ))
    limit = result.first()

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

    await db.commit()
    return {"status": "ok", "limit": str(limit.id)}


@router.get("/limits/consumption/{entity_type}/{entity_id}")
async def get_consumption_limits(
    entity_type: EntityType,
    entity_id: str,
    db: AsyncSession = Depends(get_db)
):
    q = select(ResourceConsumptionLimit).where(ResourceConsumptionLimit.entity_type == entity_type)
    if entity_type == EntityType.org:
        q = q.where(ResourceConsumptionLimit.org_id == entity_id)
    elif entity_type == EntityType.project:
        q = q.where(ResourceConsumptionLimit.project_id == entity_id)

    result = await db.exec(q)
    return [dict(
        resource=l.resource.value,
        max_total_minutes=l.max_total_minutes
    ) for l in result.all()]

import asyncio
from datetime import datetime

from fastapi import FastAPI
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

async def monitor_resources(interval_seconds: int = 60):
    """
    Async per-minute resource metering.
    Loops forever, recording branch provisioning as ResourceUsageMinute.
    """
    while True:
        async for session in _get_session():
            ts_minute = datetime.utcnow().replace(second=0, microsecond=0)

            # Fetch all online branches
            result = await session.exec(select(Branch).where(Branch.status == "online"))
            branches = result.all()

            for branch in branches:
                # Fetch provisioning for this branch
                prov_result = await session.exec(
                    select(BranchProvisioning).where(BranchProvisioning.branch_id == branch.id)
                )
                provisionings = prov_result.all()

                for p in provisionings:
                    usage = ResourceUsageMinute(
                        ts_minute=ts_minute,
                        org_id=branch.project.org_id,
                        project_id=branch.project_id,
                        branch_id=branch.id,
                        resource=p.resource,
                        amount=p.amount
                    )
                    session.add(usage)

            await session.commit()

        # Sleep until next minute
        await asyncio.sleep(interval_seconds)
