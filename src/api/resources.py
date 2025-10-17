import asyncio
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select

from ..check_branch_status import get_branch_status
from .db import SessionDep
from .models._util import Identifier
from .models.branch import Branch
from .models.project import Project
from .models.resources import (
    BranchProvisioning,
    EntityType,
    ProvisioningLog,
    ResourceConsumptionLimit,
    ResourceLimit,
    ResourceType,
    ResourceUsageMinute,
)
from .settings import settings

router = APIRouter()

# ---------------------------
# Helper functions
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


logger = logging.getLogger(__name__)


async def get_effective_branch_limits(db: AsyncSession, branch_id: Identifier) -> dict:
    result = await db.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(404, f"Branch {branch_id} not found")

    project = await branch.awaitable_attrs.project
    project_id = branch.project_id
    org_id = project.organization_id

    effective_limits = {}

    for resource in ResourceType:
        org_limit = (
            (
                await db.execute(
                    select(ResourceLimit).where(
                        ResourceLimit.entity_type == EntityType.org,
                        ResourceLimit.org_id == org_id,
                        ResourceLimit.project_id.is_(None),  # type: ignore[union-attr]
                        ResourceLimit.resource == resource,
                    )
                )
            )
            .scalars()
            .first()
        )

        project_limit = (
            (
                await db.execute(
                    select(ResourceLimit).where(
                        ResourceLimit.entity_type == EntityType.project,
                        ResourceLimit.org_id == org_id,
                        ResourceLimit.project_id == project_id,
                        ResourceLimit.resource == resource,
                    )
                )
            )
            .scalars()
            .first()
        )

        per_branch_limit = (
            project_limit.max_per_branch
            if project_limit and project_limit.max_per_branch is not None
            else org_limit.max_per_branch
            if org_limit and org_limit.max_per_branch is not None
            else 32000
        )

        # Aggregate usage
        org_prov = (
            await db.execute(
                select(func.sum(BranchProvisioning.amount))
                .join(Branch)
                .join(Project)
                .where(Project.organization_id == org_id, BranchProvisioning.resource == resource)
            )
        ).scalars().one() or 0

        proj_prov = (
            await db.execute(
                select(func.sum(BranchProvisioning.amount))
                .join(Branch)
                .where(Branch.project_id == project_id, BranchProvisioning.resource == resource)
            )
        ).scalars().one() or 0

        remaining_org = (org_limit.max_total - org_prov) if org_limit else float("inf")
        remaining_project = (project_limit.max_total - proj_prov) if project_limit else float("inf")

        effective_limits[resource.value] = max(min(per_branch_limit, remaining_org, remaining_project), 0)

    return effective_limits


async def get_total_allocated(db: AsyncSession, project_id: Identifier, resource: ResourceType) -> int:
    total = await db.execute(
        select(func.coalesce(func.sum(BranchProvisioning.amount), 0))
        .join(Branch)
        .where(Branch.project_id == project_id, BranchProvisioning.resource == resource)
    )
    return total.scalars().one() or 0


async def log_provisioning(
    db: AsyncSession, branch_id: Identifier, resource: ResourceType, amount: int, action: str, reason: str | None = None
):
    log = ProvisioningLog(
        branch_id=branch_id, resource=resource, amount=amount, action=action, reason=reason, ts=datetime.now(UTC)
    )
    db.add(log)
    await db.commit()


class RessourcesPayload(BaseModel):
    ressources: dict[str, int]


class ToFromPayload(BaseModel):
    cycle_start: datetime | None = None
    cycle_end: datetime | None = None


class ProvLimitPayload(BaseModel):
    resource: ResourceType
    max_total: int
    max_per_branch: int


class ConsumptionPayload(BaseModel):
    resource: ResourceType
    max_total_minutes: int


# ---------------------------
# Provisioning endpoints
# ---------------------------
@router.post("/branches/{branch_id}/provision")
async def provision_branch(
    session: SessionDep,
    branch_id: Identifier,
    payload: RessourcesPayload,
):
    provision = payload.ressources
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(404, "Branch not found")

    effective_limits = await get_effective_branch_limits(session, branch_id)

    for rtype, amount in provision.items():
        effective_limit = effective_limits.get(rtype)
        if effective_limit is not None and amount > effective_limit:
            raise HTTPException(422, f"{rtype} limit exceeded for branch {branch.id}")

        # total_allocated = await get_total_allocated(db, branch.project_id, rtype)
        # if effective_limit is not None and (total_allocated + amount) > effective_limit:
        #    raise HTTPException(422, f"Total allocation for {rtype.value} exceeds project/org limit")

        # Create or update provisioning
        result = await session.execute(
            select(BranchProvisioning).where(
                BranchProvisioning.branch_id == branch_id, BranchProvisioning.resource == rtype
            )
        )
        bp = result.scalars().first()
        if bp:
            bp.amount = amount
        else:
            bp = BranchProvisioning(branch_id=branch_id, resource=rtype, amount=amount, updated_at=datetime.now())
            session.add(bp)

        await session.commit()
        await log_provisioning(session, branch_id, ResourceType(rtype), amount, "create")

    return {"status": "ok"}


@router.get("/branches/{branch_id}/provision")
async def get_branch_provisioning_api(session: SessionDep, branch_id: Identifier):
    result = await session.execute(select(BranchProvisioning).where(BranchProvisioning.branch_id == branch_id))
    provisionings = result.scalars().all()
    return {p.resource.value: p.amount for p in provisionings}


# ---------------------------
# Resource usage endpoints
# ---------------------------
#


@router.get("/projects/{project_id}/usage")
async def get_project_usage(
    session: SessionDep,
    project_id: Identifier,
    payload: ToFromPayload,
):
    def normalize(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(UTC).replace(tzinfo=None)
        return dt

    cycle_start = normalize(payload.cycle_start)
    cycle_end = normalize(payload.cycle_end)

    query = select(ResourceUsageMinute).where(ResourceUsageMinute.project_id == project_id)
    if cycle_start:
        query = query.where(ResourceUsageMinute.ts_minute >= cycle_start)
    if cycle_end:
        query = query.where(ResourceUsageMinute.ts_minute < cycle_end)

    result = await session.execute(query)
    usages = result.scalars().all()

    result_dict: dict[str, int] = {}
    for u in usages:
        result_dict.setdefault(u.resource.value, 0)
        result_dict[u.resource.value] += u.amount

    return result_dict


@router.get("/organizations/{org_id}/usage")
async def get_org_usage(
    session: SessionDep,
    org_id: Identifier,
    payload: ToFromPayload,
):
    # Normalize datetimes → make them naive UTC
    def normalize(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(UTC).replace(tzinfo=None)
        return dt

    start = normalize(payload.cycle_start)
    end = normalize(payload.cycle_end)

    query = select(ResourceUsageMinute).where(ResourceUsageMinute.org_id == org_id)
    if start:
        query = query.where(ResourceUsageMinute.ts_minute >= start)
    if end:
        query = query.where(ResourceUsageMinute.ts_minute < end)

    result = await session.execute(query)
    usages = result.scalars().all()

    result_dict: dict[str, int] = {}
    for u in usages:
        result_dict.setdefault(u.resource.value, 0)
        result_dict[u.resource.value] += u.amount

    return result_dict


# ---------------------------
# Limits endpoints
# ---------------------------
@router.post("/limits/provisioning/{entity_type}/{entity_id}")
async def set_provisioning_limit(
    session: SessionDep,
    payload: ProvLimitPayload,
    entity_type: EntityType,
    entity_id: Identifier,
):
    if entity_type == EntityType.org:
        org_id, project_id = entity_id, None
    elif entity_type == EntityType.project:
        result = await session.execute(select(Project).where(Project.id == entity_id))
        project = result.scalars().first()
        if not project:
            raise HTTPException(404, "Project not found")
        org_id, project_id = project.organization_id, project.id
    else:
        raise HTTPException(400, "Unsupported entity type")

    if project_id is not None:
        result = await session.execute(
            select(ResourceLimit).where(
                ResourceLimit.entity_type == entity_type,
                ResourceLimit.org_id == org_id,
                ResourceLimit.project_id == project_id,
                ResourceLimit.resource == payload.resource,
            )
        )
    else:
        result = await session.execute(
            select(ResourceLimit).where(
                ResourceLimit.entity_type == entity_type,
                ResourceLimit.org_id == org_id,
                ResourceLimit.resource == payload.resource,
            )
        )
    limit = result.scalars().first()

    if limit:
        limit.max_total = payload.max_total
        limit.max_per_branch = payload.max_per_branch
    else:
        limit = ResourceLimit(
            entity_type=entity_type,
            org_id=org_id,
            project_id=project_id,
            resource=payload.resource,
            max_total=payload.max_total,
            max_per_branch=payload.max_per_branch,
        )
        session.add(limit)

    await session.commit()
    return {"status": "ok", "limit": str(limit.id)}


@router.get("/limits/provisioning/{entity_type}/{entity_id}")
async def get_provisioning_limits(
    session: SessionDep,
    entity_type: EntityType,
    entity_id: Identifier,
):
    q = select(ResourceLimit).where(ResourceLimit.entity_type == entity_type)
    if entity_type == EntityType.org:
        q = q.where(ResourceLimit.org_id == entity_id)
    elif entity_type == EntityType.project:
        q = q.where(ResourceLimit.project_id == entity_id)

    result = await session.execute(q)
    return [
        {"resource": limit.resource.value, "max_total": limit.max_total, "max_per_branch": limit.max_per_branch}
        for limit in result.scalars().all()
    ]


@router.post("/limits/consumption/{entity_type}/{entity_id}")
async def set_consumption_limit(
    session: SessionDep,
    entity_type: EntityType,
    entity_id: Identifier,
    payload: ConsumptionPayload,
):
    if entity_type == EntityType.org:
        org_id, project_id = entity_id, None
    elif entity_type == EntityType.project:
        result = await session.execute(select(Project).where(Project.id == entity_id))
        project = result.scalars().first()
        if not project:
            raise HTTPException(404, "Project not found")
        org_id, project_id = project.organization_id, project.id
    else:
        raise HTTPException(400, "Unsupported entity type")

    result = await session.execute(
        select(ResourceConsumptionLimit).where(
            ResourceConsumptionLimit.entity_type == entity_type,
            ResourceConsumptionLimit.org_id == org_id,
            ResourceConsumptionLimit.project_id == project_id,
            ResourceConsumptionLimit.resource == payload.resource,
        )
    )
    limit = result.scalars().first()

    if limit:
        limit.max_total_minutes = payload.max_total_minutes
    else:
        limit = ResourceConsumptionLimit(
            entity_type=entity_type,
            org_id=org_id,
            project_id=project_id,
            resource=payload.resource,
            max_total_minutes=payload.max_total_minutes,
        )
        session.add(limit)

    await session.commit()
    return {"status": "ok", "limit": str(limit.id)}


@router.get("/limits/consumption/{entity_type}/{entity_id}")
async def get_consumption_limits(
    session: SessionDep,
    entity_type: EntityType,
    entity_id: Identifier,
):
    q = select(ResourceConsumptionLimit).where(ResourceConsumptionLimit.entity_type == entity_type)
    if entity_type == EntityType.org:
        q = q.where(ResourceConsumptionLimit.org_id == entity_id)
    elif entity_type == EntityType.project:
        q = q.where(ResourceConsumptionLimit.project_id == entity_id)

    result = await session.execute(q)
    return [
        {"resource": limit.resource.value, "max_total_minutes": limit.max_total_minutes}
        for limit in result.scalars().all()
    ]


@router.get("/branches/{branch_id}/limits/")
async def branch_effective_limit(
    session: SessionDep,
    branch_id: Identifier,
):
    limit = await get_effective_branch_limits(session, branch_id)
    return limit


async def monitor_resources(interval_seconds: int = 60):
    while True:
        try:
            async with AsyncSessionLocal() as db:
                ts_minute = datetime.now(UTC).replace(second=0, microsecond=0)

                result = await db.execute(select(Branch))
                branches = result.scalars().all()
                logger.info("Found %d active branches", len(branches))

                for branch in branches:
                    status = await get_branch_status(branch)
                    if status == "ACTIVE_HEALTHY":
                        prov_result = await db.execute(
                            select(BranchProvisioning).where(BranchProvisioning.branch_id == branch.id)
                        )
                        provisionings = prov_result.scalars().all()

                        for p in provisionings:
                            project = await branch.awaitable_attrs.project
                            usage = ResourceUsageMinute(
                                ts_minute=ts_minute,
                                org_id=project.organization_id,
                                project_id=branch.project_id,
                                branch_id=branch.id,
                                resource=p.resource,
                                amount=p.amount,
                            )
                            db.add(usage)

                await db.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Error running metering monitor iteration")

        await asyncio.sleep(interval_seconds)
