import asyncio
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select

from ..check_branch_status import get_branch_status
from ._util.resourcelimit import (
    check_resource_limits,
    create_or_update_branch_provisioning,
    dict_to_resource_limits,
    get_current_branch_allocations,
    get_effective_branch_limits,
    get_organization_resource_usage,
    get_project_resource_usage,
    make_usage_cycle,
)
from .db import SessionDep
from .models._util import Identifier
from .models.branch import Branch
from .models.project import Project
from .models.resources import (
    BranchAllocationPublic,
    BranchProvisioning,
    BranchProvisionPublic,
    ConsumptionLimitPublic,
    ConsumptionPayload,
    EntityType,
    LimitResultPublic,
    ProvisioningLimitPublic,
    ProvLimitPayload,
    ResourceConsumptionLimit,
    ResourceLimit,
    ResourceLimitsPublic,
    ResourcesPayload,
    ResourceUsageMinute,
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


# ---------------------------
# Provisioning endpoints
# ---------------------------
@router.post("/branches/{branch_id}/allocations")
async def set_branch_allocations(
    session: SessionDep, branch_id: Identifier, payload: ResourcesPayload
) -> BranchProvisionPublic:
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(404, "Branch not found")

    exceeded_limits = await check_resource_limits(session, branch, ResourcesPayload.resources)
    if len(exceeded_limits) > 0:
        raise HTTPException(422, f"Branch {branch.id} limit(s) exceeded: {exceeded_limits}")

    await create_or_update_branch_provisioning(session, branch, payload.resources)

    return BranchProvisionPublic(status="ok")


@router.get("/branches/{branch_id}/allocations")
async def get_branch_allocations(session: SessionDep, branch_id: Identifier) -> BranchAllocationPublic:
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().first()
    if not branch:
        raise HTTPException(404, "Branch not found")

    return await get_current_branch_allocations(session, branch)


# ---------------------------
# Resource usage endpoints
# ---------------------------
#
@router.get("/projects/{project_id}/usage")
async def get_project_usage(
    session: SessionDep, project_id: Identifier, cycle_start: datetime | None = None, cycle_end: datetime | None = None
) -> ResourceLimitsPublic:
    usage_cycle = make_usage_cycle(cycle_start, cycle_end)
    return dict_to_resource_limits(await get_project_resource_usage(session, project_id, usage_cycle))


@router.get("/organizations/{organization_id}/usage")
async def get_org_usage(
    session: SessionDep,
    organization_id: Identifier,
    cycle_start: datetime | None = None,
    cycle_end: datetime | None = None,
) -> ResourceLimitsPublic:
    usage_cycle = make_usage_cycle(cycle_start, cycle_end)
    return dict_to_resource_limits(await get_organization_resource_usage(session, organization_id, usage_cycle))


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
async def branch_effective_limit(session: SessionDep, branch_id: Identifier) -> ResourceLimitsPublic:
    result = await session.execute(select(Branch).where(Branch.id == branch_id))
    branch = result.scalars().one()
    return await get_effective_branch_limits(session, branch)


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
    session: SessionDep, entity_type: EntityType, entity_id: Identifier | None
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
