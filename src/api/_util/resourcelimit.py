from datetime import UTC, datetime

from sqlalchemy import func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlmodel import select

from ..._util import Identifier
from ...exceptions import VelaResourceLimitError
from ..db import SessionDep
from ..models.branch import Branch
from ..models.organization import Organization
from ..models.project import Project
from ..models.resources import (
    BranchAllocationPublic,
    BranchProvisioning,
    EntityType,
    ProvisioningLog,
    ResourceLimit,
    ResourceLimitsPublic,
    ResourceType,
    ResourceUsageMinute,
    UsageCycle,
)
from ..settings import settings


async def get_current_branch_allocations(session: SessionDep, branch: Branch) -> BranchAllocationPublic:
    result = await session.execute(select(BranchProvisioning).where(BranchProvisioning.branch_id == branch.id))
    allocations = list(result.scalars().all())

    return BranchAllocationPublic(
        branch_id=branch.id,
        milli_vcpu=_select_allocation(ResourceType.milli_vcpu, allocations),
        ram=_select_allocation(ResourceType.ram, allocations),
        iops=_select_allocation(ResourceType.iops, allocations),
        database_size=_select_allocation(ResourceType.database_size, allocations),
        storage_size=_select_allocation(ResourceType.storage_size, allocations),
    )


async def audit_new_branch_resource_provisioning(
    session: SessionDep,
    branch: Branch,
    resource_type: ResourceType,
    amount: int,
    action: str,
    reason: str | None = None,
):
    new_log = ProvisioningLog(
        branch_id=branch.id,
        resource=resource_type,
        amount=amount,
        action=action,
        reason=reason,
        ts=datetime.now(UTC),
    )
    await session.merge(new_log)


async def create_or_update_branch_provisioning(
    session: SessionDep, branch: Branch, resource_requests: ResourceLimitsPublic
):
    requests = resource_limits_to_dict(resource_requests)
    for resource_type, amount in requests.items():
        if amount is None:
            continue

        # Create or update allocation
        result = await session.execute(
            select(BranchProvisioning).where(
                BranchProvisioning.branch_id == branch.id, BranchProvisioning.resource == resource_type
            )
        )
        allocation = result.scalars().first()

        new_allocation = allocation is None
        if allocation is None:
            allocation = BranchProvisioning(
                branch_id=branch.id,
                resource=resource_type,
                amount=amount,
                updated_at=datetime.now(),
            )
        else:
            allocation.amount = int(amount or 0)  # else won't happen since it's checked above
            allocation.updated_at = datetime.now()
        await session.merge(allocation)

        # Create audit log entry
        await audit_new_branch_resource_provisioning(
            session, branch, resource_type, amount, "create" if new_allocation else "update"
        )

    await session.commit()
    await session.refresh(branch)


async def clone_branch_provisioning(session: SessionDep, source: Branch, target: Branch):
    result = await session.execute(select(BranchProvisioning).where(BranchProvisioning.branch_id == source.id))
    provisions = result.scalars().all()

    with session.no_autoflush:
        for provision in provisions:
            await session.merge(
                BranchProvisioning(
                    branch_id=target.id,
                    resource=provision.resource,
                    amount=provision.amount,
                    updated_at=datetime.now(),
                )
            )
    await session.commit()
    await session.refresh(target)


async def create_system_resource_limits(conn: AsyncConnection):
    result = await conn.execute(select(ResourceLimit).where(ResourceLimit.entity_type == EntityType.system))

    # If already initialized, do nothing
    if len(list(result.scalars().all())) > 0:
        return

    # Set up initial system resource limits if not yet existing
    resource_limits = ResourceLimitsPublic(
        milli_vcpu=settings.system_limit_millis_vcpu,
        ram=settings.system_limit_ram,
        iops=settings.system_limit_iops,
        database_size=settings.system_limit_database_size,
        storage_size=settings.system_limit_storage_size,
    )
    for resource_type, limit in resource_limits.model_dump(exclude_unset=True).items():
        if limit is not None:
            await conn.execute(
                insert(ResourceLimit).values(
                    entity_type=EntityType.system,
                    org_id=None,
                    project_id=None,
                    resource=ResourceType(resource_type),
                    max_total=limit,
                    max_per_branch=limit,
                )
            )
    await conn.commit()


async def initialize_organization_resource_limits(session: SessionDep, organization: Organization):
    result = await session.execute(select(ResourceLimit).where(ResourceLimit.entity_type == EntityType.system))
    system_limits = result.scalars().all()

    with session.no_autoflush:
        for system_limit in system_limits:
            await session.merge(
                ResourceLimit(
                    entity_type=EntityType.org,
                    org_id=organization.id,
                    project_id=None,
                    resource=system_limit.resource,
                    max_total=system_limit.max_total,
                    max_per_branch=system_limit.max_per_branch,
                )
            )
    await session.commit()
    await session.refresh(organization)


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
    return _map_resource_usages(list(usages))


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
    return _map_resource_usages(list(usages))


async def check_resource_limits(
    session: SessionDep, branch: Branch, provisioning_request: ResourceLimitsPublic
) -> list[ResourceType]:
    project = await branch.awaitable_attrs.project
    project_id = branch.project_id
    organization_id = project.organization_id
    return await check_available_resources_limits(session, organization_id, project_id, provisioning_request)


async def check_available_resources_limits(
    session: SessionDep, organization_id: Identifier, project_id: Identifier, provisioning_request: ResourceLimitsPublic
) -> list[ResourceType]:
    effective_branch_limits = await get_remaining_project_resources(session, organization_id, project_id)
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
    organization_id = (await branch.awaitable_attrs.project).organization_id
    return await get_remaining_project_resources(session, organization_id, branch.project_id)


async def get_remaining_project_resources(
    session: SessionDep, organization_id: Identifier, project_id: Identifier
) -> ResourceLimitsPublic:
    system_limits = await get_system_resource_limits(session)
    organization_limits = await get_organization_resource_limits(session, organization_id)
    project_limits = await get_project_resource_limits(session, organization_id, project_id)

    organization_allocations = await get_current_organization_allocations(session, organization_id)
    project_allocations = await get_current_project_allocations(session, project_id)

    effective_limits: dict[ResourceType, int] = {}
    for resource_type in ResourceType:
        system_limit = system_limits.get(resource_type)
        organization_limit = organization_limits.get(resource_type)
        project_limit = project_limits.get(resource_type)
        per_branch_limit = (
            project_limit.max_per_branch
            if project_limit and project_limit.max_per_branch is not None
            else organization_limit.max_per_branch
            if organization_limit and organization_limit.max_per_branch is not None
            else system_limit.max_per_branch
            if system_limit and system_limit.max_per_branch is not None
            else None
        )

        if per_branch_limit is None:
            # This should never happen! If it does, we forgot to initialize the limit at organization creation.
            raise VelaResourceLimitError()

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


async def get_system_resource_limits(session: SessionDep) -> dict[ResourceType, ResourceLimit]:
    result = await session.execute(
        select(ResourceLimit).where(
            ResourceLimit.entity_type == EntityType.system,
            ResourceLimit.org_id.is_(None),  # type: ignore[union-attr]
            ResourceLimit.project_id.is_(None),  # type: ignore[union-attr]
        )
    )
    return _map_resource_limits(list(result.scalars().all()))


async def get_organization_resource_limits(
    session: SessionDep, organization_id: Identifier
) -> dict[ResourceType, ResourceLimit]:
    result = await session.execute(
        select(ResourceLimit).where(
            ResourceLimit.entity_type == EntityType.org,
            ResourceLimit.org_id == organization_id,
            ResourceLimit.project_id.is_(None),  # type: ignore[union-attr]
        )
    )
    return _map_resource_limits(list(result.scalars().all()))


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
    return _map_resource_limits(list(result.scalars().all()))


def _map_resource_limits(limits: list[ResourceLimit]) -> dict[ResourceType, ResourceLimit]:
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
    return _map_resource_allocation(list(result.scalars().all()))


async def get_current_project_allocations(session: SessionDep, project_id: Identifier) -> dict[ResourceType, int]:
    result = await session.execute(
        select(func.sum(BranchProvisioning.amount)).join(Branch).where(Branch.project_id == project_id)
    )
    return _map_resource_allocation(list(result.scalars().all()))


def _map_resource_allocation(provisioning_list: list[BranchProvisioning]) -> dict[ResourceType, int]:
    result: dict[ResourceType, int] = {}
    for resource_type in ResourceType:
        result[resource_type] = _select_resource_allocation_or_zero(resource_type, provisioning_list)
    return result


def _select_resource_allocation_or_zero(resource_type: ResourceType, allocations: list[BranchProvisioning]):
    value: int | None = None
    for allocation in allocations:
        if allocation.resource == resource_type:
            if value is not None:
                raise ValueError(f"Multiple allocations entries for resource type {resource_type.name}")
            value = allocation.amount
    return value if value is not None else 0


def _map_resource_usages(usages: list[ResourceUsageMinute]) -> dict[ResourceType, int]:
    result: dict[ResourceType, int] = {}
    for usage in usages:
        result[usage.resource] = result.get(usage.resource, 0) + usage.amount
    return result


def _select_allocation(resource_type: ResourceType, allocations: list[BranchProvisioning]):
    for allocation in allocations:
        if allocation.resource == resource_type:
            return allocation.amount
    return None
