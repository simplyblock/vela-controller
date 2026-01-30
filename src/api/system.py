import os

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import select

from .._util import (
    DB_SIZE_MAX,
    DB_SIZE_MIN,
    DB_SIZE_STEP,
    IOPS_MAX,
    IOPS_MIN,
    IOPS_STEP,
    MEMORY_MAX,
    MEMORY_MIN,
    MEMORY_STEP,
    STORAGE_SIZE_MAX,
    STORAGE_SIZE_MIN,
    STORAGE_SIZE_STEP,
    VCPU_MILLIS_MAX,
    VCPU_MILLIS_MIN,
    VCPU_MILLIS_STEP,
)
from ..models.resources import ResourceLimitDefinitionPublic, ResourceType
from ..models.role import AccessRight
from ._util.resourcelimit import get_system_resource_limits
from .auth import authenticated_user
from .db import SessionDep

api = APIRouter(tags=["system"])


class AvailablePostgresqlVersion(BaseModel):
    label: str
    value: str
    default: bool


class SystemVersion(BaseModel):
    commit_hash: str = ""
    timestamp: str = ""


@api.get("/version", response_model=SystemVersion)
async def get_system_version() -> SystemVersion:
    """
    Provide build metadata for the running controller instance.
    """
    commit_hash = os.getenv("VELA_GIT_COMMIT", "")
    timestamp = os.getenv("VELA_BUILD_TIMESTAMP", "")
    return SystemVersion(commit_hash=commit_hash, timestamp=timestamp)


@api.get("/available-permissions/", dependencies=[Depends(authenticated_user)])
async def list_available_permissions(
    session: SessionDep,
) -> list[str]:
    """
    List all access rights defined in the system.
    """
    stmt = select(AccessRight.entry)
    result = await session.execute(stmt)
    entries = [row[0] for row in result.all()]
    return entries


@api.get("/resource-limit-definitions", dependencies=[Depends(authenticated_user)])
async def list_resource_limit_definitions(
    session: SessionDep,
) -> list[ResourceLimitDefinitionPublic]:
    system_limits = await get_system_resource_limits(session)

    def _get_limit(resource_type: ResourceType, default: int) -> int:
        return system_limits[resource_type].max_total if system_limits[resource_type] else default

    max_vcpu_millis = _get_limit(ResourceType.milli_vcpu, VCPU_MILLIS_MAX)
    max_ram_bytes = _get_limit(ResourceType.ram, MEMORY_MAX)
    max_iops = _get_limit(ResourceType.iops, IOPS_MAX)
    max_database_size_bytes = _get_limit(ResourceType.database_size, DB_SIZE_MAX)
    max_storage_size_bytes = _get_limit(ResourceType.storage_size, STORAGE_SIZE_MAX)

    return [
        ResourceLimitDefinitionPublic(
            resource_type="milli_vcpu", min=VCPU_MILLIS_MIN, max=max_vcpu_millis, step=VCPU_MILLIS_STEP, unit="Millis"
        ),
        ResourceLimitDefinitionPublic(
            resource_type="ram", min=MEMORY_MIN, max=max_ram_bytes, step=MEMORY_STEP, unit="MiB"
        ),
        ResourceLimitDefinitionPublic(resource_type="iops", min=IOPS_MIN, max=max_iops, step=IOPS_STEP, unit="IOPS"),
        ResourceLimitDefinitionPublic(
            resource_type="database_size", min=DB_SIZE_MIN, max=max_database_size_bytes, step=DB_SIZE_STEP, unit="GB"
        ),
        ResourceLimitDefinitionPublic(
            resource_type="storage_size",
            min=STORAGE_SIZE_MIN,
            max=max_storage_size_bytes,
            step=STORAGE_SIZE_STEP,
            unit="GB",
        ),
    ]


@api.get("/available-postgresql-versions", dependencies=[Depends(authenticated_user)])
async def list_available_postgresql_versions() -> list[AvailablePostgresqlVersion]:
    return [
        AvailablePostgresqlVersion(
            label="15",
            value="15.1.0.147",
            default=False,
        ),
        AvailablePostgresqlVersion(
            label="18.1 (beta)",
            value="18.1-velaos",
            default=True,
        ),
    ]
