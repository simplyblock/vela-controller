import itertools
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, fields
from datetime import UTC, datetime
from typing import Self

from pydantic import model_validator
from pydantic.dataclasses import dataclass
from sqlalchemy import case as sa_case
from sqlalchemy import delete, func
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from ..._util import Identifier, single_or_none
from ...exceptions import VelaResourceLimitError
from ...models.branch import Branch, BranchServiceStatus
from ...models.organization import Organization
from ...models.project import Project
from ...models.resources import (
    BranchAllocationPublic,
    EntityType,
    OrganizationLimitDefault,
    ProvisioningLog,
    ResourceLimit,
    ResourceLimitsPublic,
    ResourceType,
    ResourceUsageMinute,
    UsageCycle,
)
from ..db import SessionDep


async def delete_branch_provisioning(session: SessionDep, branch_id: Identifier, *, commit: bool = True):
    await session.execute(delete(ResourceUsageMinute).where(col(ResourceUsageMinute.branch_id) == branch_id))
    await session.execute(delete(ProvisioningLog).where(col(ProvisioningLog.branch_id) == branch_id))

    if commit:
        await session.commit()


async def get_current_branch_allocations(_session: SessionDep, branch: Branch) -> BranchAllocationPublic:
    return BranchAllocationPublic(
        branch_id=branch.id,
        milli_vcpu=branch.milli_vcpu,
        ram=branch.memory,
        iops=branch.iops,
        database_size=branch.database_size,
        storage_size=branch.storage_size,
    )


async def audit_new_branch_resource_provisioning(
    session: SessionDep,
    branch: Branch,
    resource_type: ResourceType,
    amount: int,
    action: str,
    reason: str | None = None,
):
    timestamp = datetime.now(UTC)
    new_log = ProvisioningLog(
        branch_id=branch.id,
        resource=resource_type,
        amount=amount,
        action=action,
        reason=reason,
        ts=timestamp,
    )
    await session.merge(new_log)


async def apply_branch_resource_allocation(
    session: SessionDep,
    branch: Branch,
    resource_requests: ResourceLimitsPublic,
    *,
    commit: bool = True,
) -> None:
    field_map = {
        ResourceType.milli_vcpu: "milli_vcpu",
        ResourceType.ram: "memory",
        ResourceType.iops: "iops",
        ResourceType.database_size: "database_size",
        ResourceType.storage_size: "storage_size",
    }
    requests = resource_limits_to_dict(resource_requests)
    for resource_type, amount in requests.items():
        if amount is None:
            continue
        setattr(branch, field_map[resource_type], int(amount))
        await audit_new_branch_resource_provisioning(session, branch, resource_type, amount, "update")

    await session.merge(branch)
    if commit:
        await session.commit()
        await session.refresh(branch)


def dict_to_resource_limits(value: Mapping[ResourceType, int | None]) -> ResourceLimitsPublic:
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
        return instant.replace(tzinfo=UTC).replace(second=0, microsecond=0)
    return instant.astimezone(UTC).replace(second=0, microsecond=0)


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
) -> tuple[list[ResourceType], ResourceLimitsPublic]:
    project = await branch.awaitable_attrs.project
    project_id = branch.project_id
    organization_id = project.organization_id
    return await check_available_resources_limits(session, organization_id, project_id, provisioning_request)


async def check_available_resources_limits(
    session: SessionDep,
    organization_id: Identifier,
    project_id: Identifier,
    provisioning_request: ResourceLimitsPublic,
    *,
    exclude_branch_ids: Sequence[Identifier] | None = None,
) -> tuple[list[ResourceType], ResourceLimitsPublic]:
    effective_branch_limits = await get_remaining_project_resources(
        session,
        organization_id,
        project_id,
        exclude_branch_ids=exclude_branch_ids,
    )
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
    return exceeded_limits, effective_branch_limits


def check_resource_limit(requested: int | None, available: int | None) -> bool:
    """return true if requested is more than available"""
    if requested is None:
        return False
    if available is None:
        return True
    return requested > available


def format_limit_violation_details(
    exceeded: Iterable[ResourceType],
    requested: ResourceLimitsPublic,
    limits: ResourceLimitsPublic,
) -> str:
    details: list[str] = []
    for resource in exceeded:
        requested_value = getattr(requested, resource.value, None)
        limit_value = getattr(limits, resource.value, None)
        requested_display = str(requested_value) if requested_value is not None else "unspecified"
        limit_display = str(limit_value) if limit_value is not None else "unavailable"
        details.append(f"{resource.value}: requested {requested_display}, remaining limit {limit_display}")
    return "; ".join(details)


async def get_remaining_organization_resources(
    session: SessionDep, organization_id: Identifier, *, exclude_branch_ids: Sequence[Identifier] | None = None
) -> ResourceLimitsPublic:
    organization_limits = await get_organization_resource_limits(session, organization_id)
    organization_allocations = await get_current_organization_allocations(
        session,
        organization_id,
        exclude_branch_ids=exclude_branch_ids,
    )
    effective_limits: dict[ResourceType, int | None] = {}
    for resource_type in ResourceType:
        organization_limit = organization_limits.get(resource_type)
        current_organization_allocation = organization_allocations.get(resource_type, 0)
        effective_limits[resource_type] = (
            max(organization_limit.max_total - current_organization_allocation, 0) if organization_limit else None
        )
    return dict_to_resource_limits(effective_limits)


async def get_remaining_project_resources(
    session: SessionDep,
    organization_id: Identifier,
    project_id: Identifier,
    *,
    exclude_branch_ids: Sequence[Identifier] | None = None,
) -> ResourceLimitsPublic:
    organization_limits = await get_organization_resource_limits(session, organization_id)
    project_limits = await get_project_resource_limits(session, project_id)

    organization_allocations = await get_current_organization_allocations(
        session,
        organization_id,
        exclude_branch_ids=exclude_branch_ids,
    )
    project_allocations = await get_current_project_allocations(
        session,
        project_id,
        exclude_branch_ids=exclude_branch_ids,
    )

    effective_limits: dict[ResourceType, int] = {}
    for resource_type in ResourceType:
        organization_limit = organization_limits.get(resource_type)
        project_limit = project_limits.get(resource_type)
        per_branch_limit = (
            project_limit.max_per_branch
            if project_limit
            else organization_limit.max_per_branch
            if organization_limit
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


async def get_project_resource_limits(session: SessionDep, project_id: Identifier) -> dict[ResourceType, ResourceLimit]:
    result = await session.execute(
        select(ResourceLimit).where(
            ResourceLimit.entity_type == EntityType.project,
            ResourceLimit.project_id == project_id,
        )
    )
    return _map_resource_limits(list(result.scalars().all()))


async def get_project_limit_totals(
    session: SessionDep,
    organization_id: Identifier,
    *,
    resource_types: Iterable[ResourceType] | None = None,
    exclude_project_id: Identifier | None = None,
) -> dict[ResourceType, int]:
    """Aggregate project-level max_total limits for an organization, grouped by resource."""

    resource_filter: tuple[ResourceType, ...] | None = None
    if resource_types is not None:
        resource_filter = tuple(resource_types)
        if not resource_filter:
            return {}

    resource_column = col(ResourceLimit.resource)
    max_total_column = col(ResourceLimit.max_total)

    query = (
        select(
            resource_column,
            func.sum(max_total_column).label("reserved_total"),
        )
        .join(Project, ResourceLimit.project_id == Project.id)  # type: ignore[arg-type]
        .where(
            ResourceLimit.entity_type == EntityType.project,
            Project.organization_id == organization_id,
        )
        .group_by(resource_column)
    )
    if resource_filter is not None:
        query = query.where(resource_column.in_(resource_filter))
    if exclude_project_id is not None:
        query = query.where(ResourceLimit.project_id != exclude_project_id)

    totals: dict[ResourceType, int] = {}
    rows = await session.execute(query)
    for resource, reserved_total in rows.all():
        totals[resource] = int(reserved_total or 0)
    return totals


def _map_resource_limits(limits: list[ResourceLimit]) -> dict[ResourceType, ResourceLimit]:
    result: dict[ResourceType, ResourceLimit] = {}
    for limit in limits:
        result[limit.resource] = limit
    return result


async def get_current_organization_allocations(
    session: SessionDep,
    organization_id: Identifier,
    *,
    exclude_branch_ids: Sequence[Identifier] | None = None,
) -> dict[ResourceType, int]:
    statement = _allocations().join(Project).where(Project.organization_id == organization_id)
    if exclude_branch_ids:
        statement = statement.where(col(Branch.id).notin_(exclude_branch_ids))
    row = (await session.exec(statement)).one()
    return {
        ResourceType.milli_vcpu: row.milli_vcpu,
        ResourceType.ram: row.ram,
        ResourceType.iops: row.iops,
        ResourceType.database_size: row.database_size,
        ResourceType.storage_size: row.storage_size,
    }


async def get_current_project_allocations(
    session: SessionDep,
    project_id: Identifier,
    *,
    exclude_branch_ids: Sequence[Identifier] | None = None,
) -> dict[ResourceType, int]:
    statement = _allocations().where(Branch.project_id == project_id)
    if exclude_branch_ids:
        statement = statement.where(col(Branch.id).notin_(exclude_branch_ids))
    row = (await session.exec(statement)).one()
    return {
        ResourceType.milli_vcpu: row.milli_vcpu,
        ResourceType.ram: row.ram,
        ResourceType.iops: row.iops,
        ResourceType.database_size: row.database_size,
        ResourceType.storage_size: row.storage_size,
    }


def _map_resource_usages(usages: list[ResourceUsageMinute]) -> dict[ResourceType, int]:
    result: dict[ResourceType, int] = {}
    for usage in usages:
        result[usage.resource] = result.get(usage.resource, 0) + usage.amount
    return result


def _optional_min(a: int | None, b: int | None) -> int | None:
    values = [x for x in (a, b) if x is not None]
    return min(values) if values else None


@dataclass(frozen=True)
class Resources:
    milli_vcpu: int | None
    ram: int | None
    iops: int | None
    database_size: int | None
    storage_size: int | None

    @classmethod
    def min(cls, a: Self, b: Self) -> Self:
        return cls(
            **{field.name: _optional_min(getattr(a, field.name), getattr(b, field.name)) for field in fields(cls)}
        )

    @classmethod
    def from_database(cls, provisionings: Sequence, attribute: str) -> Self:
        def keyfunc(entity) -> str:
            return entity.resource.value

        limits_dict = {
            resource: single_or_none(items)
            for resource, items in itertools.groupby(sorted(provisionings, key=keyfunc), key=keyfunc)
        }

        return cls(
            **{
                resource.value: getattr(limit, attribute)
                if (limit := limits_dict.get(resource.value)) is not None
                else None
                for resource in ResourceType
            }
        )

    def __sub__(self: Self, other: Self) -> Self:
        assert other.complete()
        return self.__class__(
            **{
                field.name: (a - getattr(other, field.name)) if (a := getattr(self, field.name)) is not None else None
                for field in fields(self)
            }
        )

    def to_public(self) -> ResourceLimitsPublic:
        return ResourceLimitsPublic(**asdict(self))

    def complete(self) -> bool:
        return all(getattr(self, field.name) is not None for field in fields(self))


@dataclass(frozen=True)
class Limits:
    total: Resources
    per_branch: Resources

    @model_validator(mode="after")
    def coherent_limits(self) -> Self:
        if not all(
            (getattr(self.total, field.name) is None) == (getattr(self.per_branch, field.name) is None)
            for field in fields(Resources)
        ):
            raise ValueError("Resource types must be set on both `total` and `per_branch` or on neither")
        return self

    @classmethod
    def from_database(cls, limits: Sequence[ResourceLimit]) -> Self:
        return cls(
            total=Resources.from_database(limits, "max_total"),
            per_branch=Resources.from_database(limits, "max_per_branch"),
        )

    @classmethod
    def from_defaults(cls, defaults: Sequence["OrganizationLimitDefault"]) -> Self:
        return cls(
            total=Resources.from_database(defaults, "max_total"),
            per_branch=Resources.from_database(defaults, "max_per_branch"),
        )

    @classmethod
    async def organization_defaults(cls, session: AsyncSession) -> Self:
        result = await session.execute(select(OrganizationLimitDefault))
        return cls.from_defaults(list(result.scalars().all()))

    def to_database(self, entity_type: EntityType) -> list[ResourceLimit]:
        return [
            ResourceLimit(
                resource=ResourceType(field.name),
                entity_type=entity_type,
                max_total=getattr(self.total, field.name),
                max_per_branch=getattr(self.per_branch, field.name),
            )
            for field in fields(Resources)
            if (getattr(self.total, field.name) is not None) or (getattr(self.per_branch, field.name) is not None)
        ]


async def organization_limits(organization: Organization) -> Limits:
    return Limits.from_database(await organization.awaitable_attrs.limits)


async def project_limits(project: Project) -> Limits:
    return Limits.from_database(await project.awaitable_attrs.limits)


def _allocations():
    return select(  # type: ignore[call-overload]
        func.coalesce(
            func.sum(sa_case((col(Branch.status) == BranchServiceStatus.ACTIVE_HEALTHY, Branch.milli_vcpu), else_=0)), 0
        ).label("milli_vcpu"),
        func.coalesce(
            func.sum(sa_case((col(Branch.status) == BranchServiceStatus.ACTIVE_HEALTHY, Branch.memory), else_=0)), 0
        ).label("ram"),
        func.coalesce(
            func.sum(sa_case((col(Branch.status) == BranchServiceStatus.ACTIVE_HEALTHY, Branch.iops), else_=0)), 0
        ).label("iops"),
        func.coalesce(func.sum(Branch.database_size), 0).label("database_size"),
        func.coalesce(func.sum(Branch.storage_size), 0).label("storage_size"),
    )


async def organization_allocations(session: AsyncSession, organization: Organization) -> Resources:
    statement = _allocations().join(Project).where(Project.organization_id == organization.id)
    allocations = (await session.exec(statement)).one()
    return Resources(**(allocations._asdict()))


async def project_allocations(session: AsyncSession, project: Project) -> Resources:
    statement = _allocations().where(Branch.project_id == project.id)
    allocations = (await session.exec(statement)).one()
    return Resources(**(allocations._asdict()))


async def organization_available(session: AsyncSession, organization: Organization) -> Resources:
    return (await organization_limits(organization)).total - await organization_allocations(session, organization)


async def project_available(session: AsyncSession, project: Project) -> Resources:
    return Resources.min(
        (await project_limits(project)).total - await project_allocations(session, project),
        await organization_available(session, await project.awaitable_attrs.organization),
    )


async def project_branch_maxima(project: Project) -> Resources:
    """Minimum per-branch limit across the hierarchy (project > organization).

    Returns None for any field where no per-branch limit has been configured at any level.
    """
    organization = await project.awaitable_attrs.organization
    return Resources.min(
        (await project_limits(project)).per_branch,
        (await organization_limits(organization)).per_branch,
    )
