from datetime import UTC, datetime

from sqlalchemy import func
from sqlmodel import select

from ..._util import Identifier
from ..db import SessionDep
from ..models.branch import Branch
from ..models.project import Project
from ..models.resources import (
    BranchProvisioning,
    EntityType,
    ResourceLimit,
    ResourceLimitsPublic,
    ResourceType,
    ResourceUsageMinute,
    UsageCycle,
)


def dict_to_resource_limits(value: dict[ResourceType, int]) -> ResourceLimitsPublic:
    return ResourceLimitsPublic(
        milli_vcpu=value.get(ResourceType.milli_vcpu),
        ram=value.get(ResourceType.ram),
        iops=value.get(ResourceType.iops),
        database_size=value.get(ResourceType.database_size),
        storage_size=value.get(ResourceType.storage_size),
    )


def resource_limits_to_dict(value: ResourceLimitsPublic) -> dict[ResourceType, int | None]:
    return {
        ResourceType.milli_vcpu: value.milli_vcpu,
        ResourceType.ram: value.ram,
        ResourceType.iops: value.iops,
        ResourceType.database_size: value.database_size,
        ResourceType.storage_size: value.storage_size,
    }


def make_usage_cycle(start: datetime | None, end: datetime | None) -> UsageCycle:
    return UsageCycle(start=normalize_datetime_to_utc(start), end=normalize_datetime_to_utc(end))


def normalize_datetime_to_utc(instant: datetime | None) -> datetime | None:
    if instant is None:
        return None
    if instant.tzinfo is None:
        return instant.astimezone(UTC).replace(tzinfo=None)
    return instant


async def get_organization_resource_usage(
    session: SessionDep, organization_id: Identifier, usage_cycle: UsageCycle
) -> dict[ResourceType, int]:
    query = select(ResourceUsageMinute).where(ResourceUsageMinute.org_id == organization_id)
    if usage_cycle.start:
        query = query.where(ResourceUsageMinute.ts_minute >= usage_cycle.start)
    if usage_cycle.end:
        query = query.where(ResourceUsageMinute.ts_minute < usage_cycle.end)

    result = await session.execute(query)
    usages = result.scalars().all()
    return __map_resource_usages(list(usages))


async def get_project_resource_usage(
    session: SessionDep, project_id: Identifier, usage_cycle: UsageCycle
) -> dict[ResourceType, int]:
    query = select(ResourceUsageMinute).where(ResourceUsageMinute.project_id == project_id)
    if usage_cycle.start:
        query = query.where(ResourceUsageMinute.ts_minute >= usage_cycle.start)
    if usage_cycle.end:
        query = query.where(ResourceUsageMinute.ts_minute < usage_cycle.end)

    result = await session.execute(query)
    usages = result.scalars().all()
    return __map_resource_usages(list(usages))


async def check_resource_limits(
    session: SessionDep, branch: Branch, provisioning_request: ResourceLimitsPublic
) -> list[ResourceType]:
    effective_branch_limits = await get_effective_branch_limits(session, branch)
    exceeded_limits: list[ResourceType] = []
    if provisioning_request.milli_vcpu:
        if check_resource_limit(provisioning_request.milli_vcpu, effective_branch_limits.milli_vcpu):
            exceeded_limits.append(ResourceType.milli_vcpu)
        if check_resource_limit(provisioning_request.ram, effective_branch_limits.ram):
            exceeded_limits.append(ResourceType.ram)
        if check_resource_limit(provisioning_request.iops, effective_branch_limits.iops):
            exceeded_limits.append(ResourceType.iops)
        if check_resource_limit(provisioning_request.database_size, effective_branch_limits.database_size):
            exceeded_limits.append(ResourceType.database_size)
        if check_resource_limit(provisioning_request.storage_size, effective_branch_limits.storage_size):
            exceeded_limits.append(ResourceType.storage_size)
    return exceeded_limits


def check_resource_limit(requested: int | None, available: int | None) -> bool:
    if requested is None:
        return False
    if available is None:
        return True
    return requested <= available


async def get_effective_branch_limits(session: SessionDep, branch: Branch) -> ResourceLimitsPublic:
    project = await branch.awaitable_attrs.project
    project_id = branch.project_id
    organization_id = project.organization_id

    organization_limits = await get_organization_resource_limits(session, organization_id)
    project_limits = await get_project_resource_limits(session, organization_id, project_id)

    organization_allocations = await get_current_organization_allocations(session, organization_id)
    project_allocations = await get_current_project_allocations(session, project_id)

    effective_limits: dict[ResourceType, int] = {}
    for resource_type in ResourceType:
        organization_limit = organization_limits.get(resource_type)
        project_limit = project_limits.get(resource_type)
        per_branch_limit = (
            project_limit.max_per_branch
            if project_limit and project_limit.max_per_branch is not None
            else organization_limit.max_per_branch
            if organization_limit and organization_limit.max_per_branch is not None
            else 32000
        )

        # Aggregate usage
        current_organization_allocation = organization_allocations.get(resource_type)
        current_project_allocation = project_allocations.get(resource_type)

        remaining_organization = (
            (organization_limit.max_total - current_organization_allocation)
            if organization_limit and current_organization_allocation
            else float("inf")
        )
        remaining_project = (
            (project_limit.max_total - current_project_allocation)
            if project_limit and current_project_allocation
            else float("inf")
        )

        effective_limits[resource_type] = int(max(min(per_branch_limit, remaining_organization, remaining_project), 0))

    return dict_to_resource_limits(effective_limits)


async def get_organization_resource_limits(
    session: SessionDep, organization_id: Identifier
) -> dict[ResourceType, ResourceLimit]:
    result = await session.execute(
        select(ResourceLimit).where(
            ResourceLimit.entity_type == EntityType.project,
            ResourceLimit.org_id == organization_id,
            ResourceLimit.project_id.is_(None),  # type: ignore[union-attr]
        )
    )
    return __map_resource_limits(list(result.scalars().all()))


async def get_project_resource_limits(
    session: SessionDep, organization_id: Identifier, project_id: Identifier
) -> dict[ResourceType, ResourceLimit]:
    result = await session.execute(
        select(ResourceLimit).where(
            ResourceLimit.entity_type == EntityType.project,
            ResourceLimit.org_id == organization_id,
            ResourceLimit.project_id == project_id,
        )
    )
    return __map_resource_limits(list(result.scalars().all()))


def __map_resource_limits(limits: list[ResourceLimit]) -> dict[ResourceType, ResourceLimit]:
    result: dict[ResourceType, ResourceLimit] = {}
    for limit in limits:
        result[limit.resource] = limit
    return result


async def get_current_organization_allocations(
    session: SessionDep, organization_id: Identifier
) -> dict[ResourceType, int]:
    result = await session.execute(
        select(func.sum(BranchProvisioning.amount))
        .join(Branch)
        .join(Project)
        .where(Project.organization_id == organization_id)
    )
    return __map_resource_allocation(list(result.scalars().all()))


async def get_current_project_allocations(session: SessionDep, project_id: Identifier) -> dict[ResourceType, int]:
    result = await session.execute(
        select(func.sum(BranchProvisioning.amount)).join(Branch).where(Branch.project_id == project_id)
    )
    return __map_resource_allocation(list(result.scalars().all()))


def __map_resource_allocation(provisioning_list: list[BranchProvisioning]) -> dict[ResourceType, int]:
    result: dict[ResourceType, int] = {}
    for resource_type in ResourceType:
        result[resource_type] = __select_resource_allocation_or_zero(resource_type, provisioning_list)
    return result


def __select_resource_allocation_or_zero(resource_type: ResourceType, allocations: list[BranchProvisioning]):
    value: int | None = None
    for allocation in allocations:
        if allocation.resource == resource_type:
            if value is not None:
                raise ValueError(f"Multiple allocations entries for resource type {resource_type.name}")
            value = allocation.amount
    return value if value is not None else 0


def __map_resource_usages(usages: list[ResourceUsageMinute]) -> dict[ResourceType, int]:
    result: dict[ResourceType, int] = {}
    for usage in usages:
        result[usage.resource] = result.get(usage.resource, 0) + usage.amount
    return result
