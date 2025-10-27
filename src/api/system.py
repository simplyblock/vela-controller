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
from .auth import authenticated_user
from .db import SessionDep
from .models.resources import ResourceLimitDefinitionPublic
from .models.role import AccessRight

api = APIRouter(dependencies=[Depends(authenticated_user)], tags=["system"])


class AvailablePostgresqlVersion(BaseModel):
    label: str
    value: str
    default: bool


@api.get("/available-permissions/")
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


@api.get("/resource-limit-definitions")
async def list_resource_limit_definitions() -> list[ResourceLimitDefinitionPublic]:
    return [
        ResourceLimitDefinitionPublic(
            resource_type="milli_vcpu", min=VCPU_MILLIS_MIN, max=VCPU_MILLIS_MAX, step=VCPU_MILLIS_STEP, unit="Millis"
        ),
        ResourceLimitDefinitionPublic(
            resource_type="ram", min=MEMORY_MIN, max=MEMORY_MAX, step=MEMORY_STEP, unit="MiB"
        ),
        ResourceLimitDefinitionPublic(resource_type="iops", min=IOPS_MIN, max=IOPS_MAX, step=IOPS_STEP, unit="IOPS"),
        ResourceLimitDefinitionPublic(
            resource_type="database_size", min=DB_SIZE_MIN, max=DB_SIZE_MAX, step=DB_SIZE_STEP, unit="GB"
        ),
        ResourceLimitDefinitionPublic(
            resource_type="storage_size", min=STORAGE_SIZE_MIN, max=STORAGE_SIZE_MAX, step=STORAGE_SIZE_STEP, unit="GB"
        ),
    ]


@api.get("/available-postgresql-versions")
async def list_available_postgresql_versions() -> list[AvailablePostgresqlVersion]:
    return [
        AvailablePostgresqlVersion(
            label="15",
            value="15.1.0.147",
            default=True,
        )
    ]
