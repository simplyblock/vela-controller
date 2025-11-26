import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select

from .._util import quantity_to_bytes, quantity_to_milli_cpu
from ..check_branch_status import get_branch_status
from ..deployment import (
    get_db_vmi_identity,
    kube_service,
    resolve_autoscaler_volume_identifiers,
    resolve_storage_volume_identifiers,
)
from ..deployment.kubernetes._util import custom_api_client
from ..deployment.simplyblock_api import create_simplyblock_api
from ..models._util import Identifier
from ..models.branch import Branch, BranchServiceStatus, ResourceUsageDefinition
from ..models.project import Project
from ..models.resources import (
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
from ._util.resourcelimit import (
    check_resource_limits,
    create_or_update_branch_provisioning,
    dict_to_resource_limits,
    format_limit_violation_details,
    get_current_branch_allocations,
    get_effective_branch_limits,
    get_organization_resource_usage,
    get_project_resource_usage,
    make_usage_cycle,
)
from .auth import authenticated_user
from .db import SessionDep
from .organization.project.branch import refresh_branch_status
from .settings import get_settings

router = APIRouter(dependencies=[Depends(authenticated_user)], tags=["resource"])
if TYPE_CHECKING:
    from collections.abc import Sequence

router = APIRouter(tags=["resource"])


# ---------------------------
# Helper functions
# ---------------------------

engine = create_async_engine(
    str(get_settings().postgres_url),
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

    exceeded_limits, effective_limits = await check_resource_limits(session, branch, payload.resources)
    if exceeded_limits:
        violation_details = format_limit_violation_details(exceeded_limits, payload.resources, effective_limits)
        raise HTTPException(422, f"Branch {branch.id} limit(s) exceeded: {violation_details}")

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

    limit_id = limit.id
    await session.commit()
    return LimitResultPublic(status="ok", limit=limit_id)


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

    limit_id = limit.id
    await session.commit()
    return LimitResultPublic(status="ok", limit=limit_id)


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


async def _resolve_vm_pod_name(namespace: str, vm_name: str) -> str:
    pod_ref = await kube_service.get_vm_pod_name(namespace, vm_name)
    return pod_ref[0] if isinstance(pod_ref, tuple) else pod_ref


async def _fetch_pod_metrics(namespace: str, pod_name: str) -> dict[str, Any]:
    async with custom_api_client() as custom_client:
        return await custom_client.get_namespaced_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="pods",
            name=pod_name,
        )


def _parse_compute_usage(metrics: dict[str, Any]) -> tuple[int, int]:
    containers = cast("Sequence[dict[str, Any]]", metrics["containers"])
    compute_usage = next(container for container in containers if container.get("name") == "compute")

    usage = cast("dict[str, Any]", compute_usage["usage"])
    cpu_usage = quantity_to_milli_cpu(usage["cpu"])
    memory_usage = quantity_to_bytes(usage["memory"])

    if cpu_usage is None or memory_usage is None:
        raise ValueError("Metrics API returned empty resource usage for compute container")

    return cpu_usage, memory_usage


async def _collect_compute_usage(namespace: str, vm_name: str) -> tuple[int, int]:
    try:
        pod_name = await _resolve_vm_pod_name(namespace, vm_name)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to resolve VM pod while collecting compute usage for {vm_name!r} in namespace {namespace!r}"
        ) from exc

    metrics = await _fetch_pod_metrics(namespace, pod_name)

    return _parse_compute_usage(metrics)


async def _resolve_volume_stats(
    *,
    volume_identifier_resolver: Callable[[str], Awaitable[tuple[str, str | None]]],
    namespace: str,
) -> dict[str, int]:
    volume_uuid, _ = await volume_identifier_resolver(namespace)

    async with create_simplyblock_api() as sb_api:
        return await sb_api.volume_iostats(volume_uuid=volume_uuid)


async def _collect_database_volume_usage(namespace: str) -> tuple[int, int]:
    stats = await _resolve_volume_stats(
        volume_identifier_resolver=resolve_autoscaler_volume_identifiers,
        namespace=namespace,
    )
    nvme_bytes = stats["size_used"]
    read_iops = stats["read_io_ps"]
    write_iops = stats["write_io_ps"]
    return nvme_bytes, read_iops + write_iops


async def _collect_storage_volume_usage(namespace: str) -> int:
    stats = await _resolve_volume_stats(
        volume_identifier_resolver=resolve_storage_volume_identifiers,
        namespace=namespace,
    )
    return stats["size_used"]


async def _collect_branch_volume_usage(branch: Branch, namespace: str) -> tuple[int, int, int | None]:
    db_task = _collect_database_volume_usage(namespace)
    if branch.enable_file_storage:
        storage_task = _collect_storage_volume_usage(namespace)
        (nvme_bytes, iops), storage_bytes = await asyncio.gather(db_task, storage_task)
    else:
        nvme_bytes, iops = await db_task
        storage_bytes = None

    return nvme_bytes, iops, storage_bytes


async def _collect_branch_resource_usage(branch: Branch) -> ResourceUsageDefinition:
    namespace, vm_name = get_db_vmi_identity(branch.id)
    milli_vcpu, ram_bytes = await _collect_compute_usage(namespace, vm_name)
    nvme_bytes, iops, storage_bytes = await _collect_branch_volume_usage(branch, namespace)

    return ResourceUsageDefinition(
        milli_vcpu=milli_vcpu,
        ram_bytes=ram_bytes,
        nvme_bytes=nvme_bytes,
        iops=iops,
        storage_bytes=storage_bytes,
    )


async def monitor_resources(interval_seconds: int = 60):
    while True:
        try:
            async with AsyncSessionLocal() as db:
                ts_minute = datetime.now(UTC).replace(second=0, microsecond=0)

                result = await db.execute(select(Branch))
                branches = result.scalars().all()
                logger.info("Found %d branches", len(branches))

                for branch in branches:
                    status = await refresh_branch_status(branch.id)
                    if status != BranchServiceStatus.ACTIVE_HEALTHY:
                        logger.debug(
                            "Skipping resource collection for branch %s with non-active status %s",
                            branch.id,
                            status,
                        )
                        continue
                    try:
                        usage = await _collect_branch_resource_usage(branch)
                    except Exception:
                        logger.exception("Failed to collect resource usage for branch %s", branch.id)
                        continue
                    branch.store_resource_usage(usage)

                    status = await get_branch_status(branch.id)
                    if status == BranchServiceStatus.ACTIVE_HEALTHY:
                        prov_result = await db.execute(
                            select(BranchProvisioning).where(BranchProvisioning.branch_id == branch.id)
                        )
                        provisionings = prov_result.scalars().all()

                        for p in provisionings:
                            project = await branch.awaitable_attrs.project
                            minute_usage = ResourceUsageMinute(
                                ts_minute=ts_minute,
                                org_id=project.organization_id,
                                project_id=branch.project_id,
                                branch_id=branch.id,
                                resource=p.resource,
                                amount=p.amount,
                            )
                            db.add(minute_usage)

                await db.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Error running metering monitor iteration")

        await asyncio.sleep(interval_seconds)
