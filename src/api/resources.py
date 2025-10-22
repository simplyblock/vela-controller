import asyncio
import logging
from datetime import UTC, datetime
from typing import get_args

from fastapi import APIRouter, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select

from ..check_branch_status import get_branch_status
from ._util.resourcelimit import check_resource_limits, get_effective_branch_limits
from .db import SessionDep
from .models._util import Identifier
from .models.branch import Branch
from .models.project import Project
from .models.resources import (
    BranchLimitsPublic,
    BranchProvisioning,
    BranchProvisionPublic,
    ConsumptionLimitPublic,
    ConsumptionPayload,
    EntityType,
    LimitResultPublic,
    ProvisioningLimitPublic,
    ProvisioningLog,
    ProvLimitPayload,
    ResourceConsumptionLimit,
    ResourceLimit,
    ResourcesPayload,
    ResourceType,
    ResourceTypePublic,
    ResourceUsageMinute,
    ToFromPayload,
)
from .settings import settings

router = APIRouter(tags=["resource"])

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


async def log_provisioning(
    db: AsyncSession, branch_id: Identifier, resource: ResourceType, amount: int, action: str, reason: str | None = None
):
    log = ProvisioningLog(
        branch_id=branch_id, resource=resource, amount=amount, action=action, reason=reason, ts=datetime.now(UTC)
    )
    db.add(log)
    await db.commit()


# ---------------------------
# Provisioning endpoints
# ---------------------------
@router.post("/branches/{branch_id}/provision")
async def provision_branch(
    session: SessionDep, branch_id: Identifier, payload: ResourcesPayload
) -> BranchProvisionPublic:
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(404, "Branch not found")

    exceeded_limits = await check_resource_limits(session, branch, ResourcesPayload.resources)
    if len(exceeded_limits) > 0:
        raise HTTPException(422, f"Branch {branch.id} limit(s) exceeded: {exceeded_limits}")

    for key in ResourcesPayload.resources:
        if payload.resources[key] is None:
            continue

        resource_type = ResourceType(key)
        amount = payload.resources[key]

        # Create or update provisioning
        result = await session.execute(
            select(BranchProvisioning).where(
                BranchProvisioning.branch_id == branch_id, BranchProvisioning.resource == resource_type
            )
        )
        bp = result.scalars().first()
        if bp:
            bp.amount = amount
        else:
            bp = BranchProvisioning(
                branch_id=branch_id, resource=resource_type, amount=amount, updated_at=datetime.now()
            )
            session.add(bp)

        await session.commit()
        await log_provisioning(session, branch_id, resource_type, amount, "create")

    return BranchProvisionPublic(status="ok")


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
    session: SessionDep, project_id: Identifier, payload: ToFromPayload
) -> dict[ResourceTypePublic, int]:
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

    result_dict: dict[ResourceTypePublic, int] = dict.fromkeys(get_args(ResourceTypePublic), 0)
    for usage in usages:
        result_dict[usage.resource.name] += usage.amount

    return result_dict


@router.get("/organizations/{organization_id}/usage")
async def get_org_usage(
    session: SessionDep, organization_id: Identifier, payload: ToFromPayload
) -> dict[ResourceTypePublic, int]:
    # Normalize datetimes → make them naive UTC
    def normalize(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(UTC).replace(tzinfo=None)
        return dt

    start = normalize(payload.cycle_start)
    end = normalize(payload.cycle_end)

    query = select(ResourceUsageMinute).where(ResourceUsageMinute.org_id == organization_id)
    if start:
        query = query.where(ResourceUsageMinute.ts_minute >= start)
    if end:
        query = query.where(ResourceUsageMinute.ts_minute < end)

    result = await session.execute(query)
    usages = result.scalars().all()

    result_dict: dict[ResourceTypePublic, int] = dict.fromkeys(get_args(ResourceTypePublic), 0)
    for usage in usages:
        result_dict[usage.resource.name] += usage.amount

    return result_dict


# ---------------------------
# Limits endpoints
# ---------------------------
@router.post("/organizations/{organization_id}/limits/provisioning")
async def set_organization_provisioning_limit(
    session: SessionDep, organization_id: Identifier, payload: ProvLimitPayload
) -> LimitResultPublic:
    return await set_provisioning_limit(session, EntityType.org, organization_id, payload)


@router.get("/organizations/{organization_id}/limits/provisioning")
async def get_organization_provisioning_limits(
    session: SessionDep, organization_id: Identifier
) -> list[ProvisioningLimitPublic]:
    return await get_provisioning_limits(session, EntityType.org, organization_id)


@router.post("/projects/{project_id}/limits/provisioning")
async def set_project_provisioning_limit(
    session: SessionDep, project_id: Identifier, payload: ProvLimitPayload
) -> LimitResultPublic:
    return await set_provisioning_limit(session, EntityType.project, project_id, payload)


@router.get("/projects/{project_id}/limits/provisioning")
async def get_project_provisioning_limits(session: SessionDep, project_id: Identifier) -> list[ProvisioningLimitPublic]:
    return await get_provisioning_limits(session, EntityType.project, project_id)


@router.post("/organizations/{organization_id}/limits/consumption")
async def set_organization_consumption_limit(
    session: SessionDep, organization_id: Identifier, payload: ConsumptionPayload
) -> LimitResultPublic:
    return await set_consumption_limit(session, EntityType.org, organization_id, payload)


@router.get("/organizations/{organization_id}/limits/consumption")
async def get_organization_consumption_limits(
    session: SessionDep, organization_id: Identifier
) -> list[ConsumptionLimitPublic]:
    return await get_consumption_limits(session, EntityType.org, organization_id)


@router.post("/projects/{project_id}/limits/consumption")
async def set_project_consumption_limit(
    session: SessionDep, project_id: Identifier, payload: ConsumptionPayload
) -> LimitResultPublic:
    return await set_consumption_limit(session, EntityType.project, project_id, payload)


@router.get("/projects/{project_id}/limits/consumption")
async def get_project_consumption_limits(session: SessionDep, project_id: Identifier) -> list[ConsumptionLimitPublic]:
    return await get_consumption_limits(session, EntityType.project, project_id)


@router.get("/branches/{branch_id}/limits/")
async def branch_effective_limit(session: SessionDep, branch_id: Identifier) -> BranchLimitsPublic:
    limits = await get_effective_branch_limits(session, branch_id)
    return BranchLimitsPublic(
        milli_vcpu=limits[ResourceType.milli_vcpu],
        ram=limits[ResourceType.ram],
        iops=limits[ResourceType.iops],
        database_size=limits[ResourceType.database_size],
        storage_size=limits[ResourceType.storage_size],
    )


async def set_provisioning_limit(
    session: SessionDep, entity_type: EntityType, entity_id: Identifier, payload: ProvLimitPayload
) -> LimitResultPublic:
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
    return LimitResultPublic(status="ok", limit=limit.id)


async def get_provisioning_limits(
    session: SessionDep, entity_type: EntityType, entity_id: Identifier
) -> list[ProvisioningLimitPublic]:
    q = select(ResourceLimit).where(ResourceLimit.entity_type == entity_type)
    if entity_type == EntityType.org:
        q = q.where(ResourceLimit.org_id == entity_id)
    elif entity_type == EntityType.project:
        q = q.where(ResourceLimit.project_id == entity_id)

    result = await session.execute(q)
    return [
        ProvisioningLimitPublic(
            resource=limit.resource.value, max_total=limit.max_total, max_per_branch=limit.max_per_branch
        )
        for limit in result.scalars().all()
    ]


async def set_consumption_limit(
    session: SessionDep, entity_type: EntityType, entity_id: Identifier, payload: ConsumptionPayload
) -> LimitResultPublic:
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
    return LimitResultPublic(status="ok", limit=limit.id)


async def get_consumption_limits(
    session: SessionDep, entity_type: EntityType, entity_id: Identifier
) -> list[ConsumptionLimitPublic]:
    q = select(ResourceConsumptionLimit).where(ResourceConsumptionLimit.entity_type == entity_type)
    if entity_type == EntityType.org:
        q = q.where(ResourceConsumptionLimit.org_id == entity_id)
    elif entity_type == EntityType.project:
        q = q.where(ResourceConsumptionLimit.project_id == entity_id)

    result = await session.execute(q)
    return [
        ConsumptionLimitPublic(resource=limit.resource.value, max_total_minutes=limit.max_total_minutes)
        for limit in result.scalars().all()
    ]


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
