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
    ResourceRequest,
    ResourceType,
)


async def check_resource_limits(
    session: SessionDep, branch: Branch, provisioning_request: ResourceRequest
) -> list[ResourceType]:
    effective_branch_limits = await get_effective_branch_limits(session, branch)
    exceeded_limits: list[ResourceType] = []
    for key, value in provisioning_request:
        if value is None:
            continue
        resource_type = ResourceType(key)
        if effective_branch_limits[resource_type] < value:
            exceeded_limits.append(resource_type)
    return exceeded_limits


async def get_effective_branch_limits(session: SessionDep, branch: Branch) -> dict[ResourceType, int]:
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
            (organization_limit.max_total - current_organization_allocation) if organization_limit else float("inf")
        )
        remaining_project = (project_limit.max_total - current_project_allocation) if project_limit else float("inf")

        effective_limits[resource_type] = max(min(per_branch_limit, remaining_organization, remaining_project), 0)

    return effective_limits


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
    return __map_resource_allocation(result.scalars().all())


async def get_current_project_allocations(session: SessionDep, project_id: Identifier) -> dict[ResourceType, int]:
    result = await session.execute(
        select(func.sum(BranchProvisioning.amount)).join(Branch).where(Branch.project_id == project_id)
    )
    return __map_resource_allocation(result.scalars().all())


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
