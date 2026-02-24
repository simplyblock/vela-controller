import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import select

from .._util import quantity_to_bytes, quantity_to_milli_cpu
from ..check_branch_status import get_branch_status
from ..deployment import (
    get_autoscaler_vm_identity,
    resolve_autoscaler_volume_identifiers,
    resolve_storage_volume_identifiers,
)
from ..deployment.kubernetes._util import custom_api_client
from ..deployment.kubernetes.neonvm import resolve_autoscaler_vm_pod_name
from ..deployment.simplyblock_api import create_simplyblock_api
from ..exceptions import VelaSimplyblockAPIError
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
    ResourceUsageMinute,
)
from ._util.resourcelimit import (
    check_resource_limits,
    create_or_update_branch_provisioning,
    dict_to_resource_limits,
    format_limit_violation_details,
    get_current_branch_allocations,
    get_effective_branch_creation_limits,
    get_effective_branch_limits,
    get_effective_project_creation_limits,
    get_organization_resource_usage,
    get_project_resource_usage,
    make_usage_cycle,
)
from .auth import authenticated_user
from .db import SessionDep
from .dependencies import BranchDep, OrganizationDep, ProjectDep
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
    session: SessionDep, branch: BranchDep, payload: ResourceLimitsPublic
) -> BranchProvisionPublic:
    exceeded_limits, effective_limits = await check_resource_limits(session, branch, payload)
    if exceeded_limits:
        violation_details = format_limit_violation_details(exceeded_limits, payload, effective_limits)
        raise HTTPException(422, f"Branch {branch.id} limit(s) exceeded: {violation_details}")

    await create_or_update_branch_provisioning(session, branch, payload)

    return BranchProvisionPublic(status="ok")


@router.get("/branches/{branch_id}/allocations")
async def get_branch_allocations(session: SessionDep, branch: BranchDep) -> BranchAllocationPublic:
    return await get_current_branch_allocations(session, branch)


# ---------------------------
# Resource usage endpoints
# ---------------------------
#
@router.get("/projects/{project_id}/usage")
async def get_project_usage(
    session: SessionDep, project: ProjectDep, cycle_start: datetime | None = None, cycle_end: datetime | None = None
) -> ResourceLimitsPublic:
    usage_cycle = make_usage_cycle(cycle_start, cycle_end)
    return dict_to_resource_limits(await get_project_resource_usage(session, project.id, usage_cycle))


@router.get("/organizations/{organization_id}/usage")
async def get_org_usage(
    session: SessionDep,
    organization: OrganizationDep,
    cycle_start: datetime | None = None,
    cycle_end: datetime | None = None,
) -> ResourceLimitsPublic:
    usage_cycle = make_usage_cycle(cycle_start, cycle_end)
    return dict_to_resource_limits(await get_organization_resource_usage(session, organization.id, usage_cycle))


# ---------------------------
# Limits endpoints
# ---------------------------
@router.get("/organizations/{organization_id}/provisioning/available")
async def get_available_organization_provisioning_resources(
    session: SessionDep, organization: OrganizationDep
) -> ResourceLimitsPublic:
    return await get_effective_project_creation_limits(session, organization)


@router.post("/organizations/{organization_id}/limits/provisioning")
async def set_organization_provisioning_limit(
    session: SessionDep, organization: OrganizationDep, payload: ProvLimitPayload
) -> LimitResultPublic:
    if (
        limit := next(
            (limit for limit in await organization.awaitable_attrs.limits if limit.resource.value == payload.resource),
            None,
        )
    ) is not None:
        limit.max_total = payload.max_total
        limit.max_per_branch = payload.max_per_branch
    else:
        limit = ResourceLimit(
            entity_type=EntityType.org,
            resource=payload.resource,
            org_id=organization.id,
            max_total=payload.max_total,
            max_per_branch=payload.max_per_branch,
        )
        session.add(limit)
    await session.commit()

    return LimitResultPublic(status="ok", limit=await limit.awaitable_attrs.id)


@router.get("/organizations/{organization_id}/limits/provisioning")
async def get_organization_provisioning_limits(organization: OrganizationDep) -> list[ProvisioningLimitPublic]:
    return [ProvisioningLimitPublic.from_limit(limit) for limit in (await organization.awaitable_attrs.limits)]


@router.get("/projects/{project_id}/provisioning/available")
async def get_available_project_provisioning_resources(
    session: SessionDep, project: ProjectDep
) -> ResourceLimitsPublic:
    return await get_effective_branch_creation_limits(session, project)


@router.post("/projects/{project_id}/limits/provisioning")
async def set_project_provisioning_limit(
    session: SessionDep, project: ProjectDep, payload: ProvLimitPayload
) -> LimitResultPublic:
    if (
        limit := next(
            (limit for limit in (await project.awaitable_attrs.limits) if (limit.resource.value == payload.resource)),
            None,
        )
    ) is not None:
        print(limit)
        limit.max_total = payload.max_total
        limit.max_per_branch = payload.max_per_branch
    else:
        limit = ResourceLimit(
            entity_type=EntityType.project,
            resource=payload.resource,
            project_id=project.id,
            max_total=payload.max_total,
            max_per_branch=payload.max_per_branch,
        )
        session.add(limit)
    await session.commit()

    return LimitResultPublic(status="ok", limit=await limit.awaitable_attrs.id)


@router.get("/projects/{project_id}/limits/provisioning")
async def get_project_provisioning_limits(project: ProjectDep) -> list[ProvisioningLimitPublic]:
    return [ProvisioningLimitPublic.from_limit(limit) for limit in (await project.awaitable_attrs.limits)]


@router.post("/organizations/{organization_id}/limits/consumption")
async def set_organization_consumption_limit(
    session: SessionDep, organization: OrganizationDep, payload: ConsumptionPayload
) -> LimitResultPublic:
    return await set_consumption_limit(session, EntityType.org, organization.id, payload)


@router.get("/organizations/{organization_id}/limits/consumption")
async def get_organization_consumption_limits(
    session: SessionDep, organization: OrganizationDep
) -> list[ConsumptionLimitPublic]:
    return await get_consumption_limits(session, EntityType.org, organization.id)


@router.post("/projects/{project_id}/limits/consumption")
async def set_project_consumption_limit(
    session: SessionDep, project: ProjectDep, payload: ConsumptionPayload
) -> LimitResultPublic:
    return await set_consumption_limit(session, EntityType.project, project.id, payload)


@router.get("/projects/{project_id}/limits/consumption")
async def get_project_consumption_limits(session: SessionDep, project: ProjectDep) -> list[ConsumptionLimitPublic]:
    return await get_consumption_limits(session, EntityType.project, project.id)


@router.get("/branches/{branch_id}/limits/")
async def branch_effective_limit(session: SessionDep, branch: BranchDep) -> ResourceLimitsPublic:
    return await get_effective_branch_limits(session, branch)


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
    compute_usage = next(container for container in containers if container.get("name") == "neonvm-runner")

    usage = cast("dict[str, Any]", compute_usage["usage"])
    cpu_usage = quantity_to_milli_cpu(usage["cpu"])
    memory_usage = quantity_to_bytes(usage["memory"])

    if cpu_usage is None or memory_usage is None:
        raise ValueError("Metrics API returned empty resource usage for compute container")

    return cpu_usage, memory_usage


async def _collect_compute_usage(namespace: str, vm_name: str) -> tuple[int, int]:
    try:
        pod_name = await resolve_autoscaler_vm_pod_name(namespace, vm_name)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to resolve VM pod while collecting compute usage for {vm_name!r} in namespace {namespace!r}"
        ) from exc

    metrics = await _fetch_pod_metrics(namespace, pod_name)

    return _parse_compute_usage(metrics)


async def _resolve_volume_stats(
    *,
    volume_identifier_resolver: Callable[[str], Awaitable[tuple[UUID, UUID | None]]],
    namespace: str,
) -> dict[str, int]:
    volume, _ = await volume_identifier_resolver(namespace)

    async with create_simplyblock_api() as sb_api:
        return await sb_api.volume_iostats(volume=volume)


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


async def _collect_branch_resource_usage(branch: Branch) -> ResourceUsageDefinition | None:
    namespace, vm_name = get_autoscaler_vm_identity(branch.id)
    try:
        compute_usage = await _collect_compute_usage(namespace, vm_name)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning(
                "Pod metrics not available yet for branch %s (namespace %s, vm %s); skipping resource usage collection",
                branch.id,
                namespace,
                vm_name,
            )
            return None
        raise

    milli_vcpu, ram_bytes = compute_usage
    nvme_bytes, iops, storage_bytes = 0, 0, None
    try:
        nvme_bytes, iops, storage_bytes = await _collect_branch_volume_usage(branch, namespace)
    except VelaSimplyblockAPIError as exc:
        logger.error(
            "Failed to collect volume stats for branch %s (namespace %s): %s",
            branch.id,
            namespace,
            exc,
        )

    return ResourceUsageDefinition(
        milli_vcpu=milli_vcpu,
        ram_bytes=ram_bytes,
        nvme_bytes=nvme_bytes,
        iops=iops,
        storage_bytes=storage_bytes,
    )


async def monitor_resources():
    interval = get_settings().resource_monitor_interval

    while True:
        start = datetime.now()
        try:
            async with AsyncSessionLocal() as db:
                ts_minute = datetime.now(UTC).replace(second=0, microsecond=0)

                result = await db.execute(select(Branch))
                branches = result.scalars().all()
                logger.info("Found %d branches", len(branches))

                for branch in branches:
                    status = await refresh_branch_status(branch.id)
                    if status not in [BranchServiceStatus.ACTIVE_HEALTHY, BranchServiceStatus.RESIZING]:
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
                    if usage is None:
                        continue
                    branch.store_resource_usage(usage)

                    status = await get_branch_status(branch.id)
                    if status in [BranchServiceStatus.ACTIVE_HEALTHY, BranchServiceStatus.RESIZING]:
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
                                original_project_id=branch.project_id,
                                branch_id=branch.id,
                                original_branch_id=branch.id,
                                resource=p.resource,
                                amount=p.amount,
                            )
                            db.add(minute_usage)

                await db.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Error running metering monitor iteration")

        elapsed = datetime.now() - start
        if elapsed < interval:
            await asyncio.sleep((interval - elapsed).total_seconds())
        else:
            logger.warning("Resource monitor execution exeeded desired interval")
