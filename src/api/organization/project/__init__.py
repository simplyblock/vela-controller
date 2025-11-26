from collections.abc import Sequence
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ...._util import Identifier
from ....deployment import delete_deployment, get_autoscaler_vm_identity, get_db_vmi_identity
from ....deployment.kubernetes.kubevirt import call_kubevirt_subresource
from ....deployment.kubernetes.neonvm import set_virtualmachine_power_state
from ....exceptions import VelaKubernetesError
from ....models.organization import Organization
from ....models.project import (
    Project,
    ProjectCreate,
    ProjectPublic,
    ProjectUpdate,
)
from ....models.resources import EntityType, ResourceLimit, ResourceType
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..._util.resourcelimit import (
    delete_branch_provisioning,
    get_organization_resource_limits,
    get_project_limit_totals,
)
from ...auth import security
from ...db import SessionDep
from ...dependencies import OrganizationDep, ProjectDep
from . import branch as branch_module

api = APIRouter(tags=["project"])


async def _public(project: Project) -> ProjectPublic:
    return ProjectPublic(
        organization_id=await project.awaitable_attrs.organization_id,
        id=await project.awaitable_attrs.id,
        name=await project.awaitable_attrs.name,
        max_backups=await project.awaitable_attrs.max_backups,
        status=await project.awaitable_attrs.status,
    )


@api.get(
    "/",
    name="organizations:projects:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_(session: SessionDep, organization: OrganizationDep) -> Sequence[ProjectPublic]:
    await session.refresh(organization, ["projects"])
    projects = await organization.awaitable_attrs.projects
    return [await _public(project) for project in projects]


_links = {
    "detail": {
        "operationId": "organizations:projects:detail",
        "parameters": {"project_id": "$response.header.Location#regex:/projects/(.+)/"},
    },
    "update": {
        "operationId": "organizations:projects:update",
        "parameters": {"project_id": "$response.header.Location#regex:/projects/(.+)/"},
    },
    "delete": {
        "operationId": "organizations:projects:delete",
        "parameters": {"project_id": "$response.header.Location#regex:/projects/(.+)/"},
    },
}


def _validation_error_detail(
    error_type: str,
    loc: Sequence[str],
    msg: str,
    input_value: Any | None = None,
    ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    detail: dict[str, Any] = {"type": error_type, "loc": list(loc), "msg": msg}
    if input_value is not None:
        detail["input"] = input_value
    if ctx:
        detail["ctx"] = ctx
    return detail


async def _validate_project_backup_budget(
    session: SessionDep,
    organization: Organization,
    requested_max_backups: int,
    *,
    exclude_project_id: Identifier | None = None,
) -> None:
    stmt = select(func.coalesce(func.sum(Project.max_backups), 0)).where(Project.organization_id == organization.id)
    if exclude_project_id is not None:
        stmt = stmt.where(Project.id != exclude_project_id)

    result = await session.execute(stmt)
    allocated = int(result.scalar_one() or 0)
    remaining_capacity = max(organization.max_backups - allocated, 0)

    if requested_max_backups > remaining_capacity:
        raise HTTPException(
            422,
            {
                "detail": [
                    _validation_error_detail(
                        "exceeded",
                        ["body", "max_backups"],
                        (
                            f"Requested max_backups {requested_max_backups} exceeds the organization's remaining backup"
                            f" capacity {remaining_capacity} (limit {organization.max_backups})"
                        ),
                        input_value=requested_max_backups,
                        ctx={
                            "limit": organization.max_backups,
                            "remaining_capacity": remaining_capacity,
                        },
                    )
                ]
            },
        )


def _resource_type_from_name(resource_name: str, field_name: str) -> ResourceType:
    try:
        return ResourceType(resource_name)
    except ValueError as exc:  # pragma: no cover - defensive coding against invalid payloads
        raise HTTPException(
            422,
            {
                "detail": [
                    _validation_error_detail(
                        "unknown_resource",
                        ["body", field_name, resource_name],
                        f"Unknown resource type {resource_name!r}",
                        input_value=resource_name,
                    )
                ]
            },
        ) from exc


def _normalize_limits(limit_payload: dict[str, int], field_name: str) -> dict[ResourceType, int]:
    normalized: dict[ResourceType, int] = {}
    for resource_name, value in limit_payload.items():
        normalized[_resource_type_from_name(resource_name, field_name)] = value
    return normalized


async def _get_consumed_project_limits(
    session: SessionDep,
    organization_id: Identifier,
    project_limits: dict[ResourceType, int],
) -> dict[ResourceType, int]:
    if not project_limits:
        return {}
    return await get_project_limit_totals(
        session,
        organization_id,
        resource_types=tuple(project_limits.keys()),
    )


def _calculate_project_limits(
    project_limits: dict[ResourceType, int],
    per_branch_limits: dict[ResourceType, int],
    org_limits: dict[ResourceType, ResourceLimit],
    consumed_limits: dict[ResourceType, int],
) -> dict[ResourceType, tuple[int, int]]:
    """Return the validated project/per-branch limits for each requested resource."""

    calculated_limits: dict[ResourceType, tuple[int, int]] = {}
    errors: list[dict[str, Any]] = []
    for resource_type, requested_limit in project_limits.items():
        limit_pair = _calculate_project_limit_pair(
            resource_type,
            requested_limit,
            org_limits.get(resource_type),
            per_branch_limits.get(resource_type),
            consumed_limits.get(resource_type, 0),
            errors,
        )
        if limit_pair is not None:
            calculated_limits[resource_type] = limit_pair
    if errors:
        raise HTTPException(422, {"detail": errors})
    return calculated_limits


def _calculate_project_limit_pair(
    resource_type: ResourceType,
    requested_limit: int,
    organization_limit: ResourceLimit | None,
    per_branch_override: int | None,
    consumed_total: int,
    errors: list[dict[str, Any]],
) -> tuple[int, int] | None:
    """Ensure the requested project limit fits the remaining org capacity."""

    resource_name = resource_type.value
    if organization_limit is None or organization_limit.max_total is None:
        errors.append(
            _validation_error_detail(
                "limit_unconfigured",
                ["body", "project_limits", resource_name],
                (
                    f"Organization limit for {resource_name} is not configured; "
                    f"cannot set project limit to {requested_limit}"
                ),
                input_value=requested_limit,
            )
        )
        return None

    # Remaining capacity is what's left once other projects have reserved their share.
    remaining_capacity = max(organization_limit.max_total - consumed_total, 0)
    if requested_limit > remaining_capacity:
        errors.append(
            _validation_error_detail(
                "exceeded",
                ["body", "project_limits", resource_name],
                (
                    f"Requested limit {requested_limit} exceeds remaining organization "
                    f"capacity {remaining_capacity} "
                    f"(limit {organization_limit.max_total})"
                ),
                input_value=requested_limit,
                ctx={
                    "limit": organization_limit.max_total,
                    "remaining_capacity": remaining_capacity,
                },
            )
        )
        return None

    per_branch_limit = _resolve_per_branch_limit(
        resource_name,
        requested_limit,
        per_branch_override,
        organization_limit.max_per_branch,
        errors,
    )
    if per_branch_limit is None:
        return None
    return requested_limit, per_branch_limit


def _resolve_per_branch_limit(
    resource_name: str,
    requested_limit: int,
    per_branch_override: int | None,
    org_per_branch_limit: int | None,
    errors: list[dict[str, Any]],
) -> int | None:
    """Pick the per-branch limit, keeping it within org policy and project total."""

    if (
        per_branch_override is not None
        and org_per_branch_limit is not None
        and per_branch_override > org_per_branch_limit
    ):
        errors.append(
            _validation_error_detail(
                "exceeded",
                ["body", "per_branch_limits", resource_name],
                (f"Requested per-branch limit {per_branch_override} exceeds organization limit {org_per_branch_limit}"),
                input_value=per_branch_override,
                ctx={"limit": org_per_branch_limit},
            )
        )
        return None

    if per_branch_override is not None:
        per_branch_limit = per_branch_override
    elif org_per_branch_limit is not None:
        per_branch_limit = org_per_branch_limit
    else:
        per_branch_limit = requested_limit

    # A branch cannot exceed the total resources allocated to the project.
    return min(per_branch_limit, requested_limit)


def _resource_limits_from_limits(
    organization_id: Identifier,
    project_id: Identifier,
    calculated_limits: dict[ResourceType, tuple[int, int]],
) -> list[ResourceLimit]:
    project_resource_limits: list[ResourceLimit] = []
    for resource_type, (project_limit, per_branch_limit) in calculated_limits.items():
        project_resource_limits.append(
            ResourceLimit(
                entity_type=EntityType.project,
                org_id=organization_id,
                project_id=project_id,
                resource=resource_type,
                max_total=project_limit,
                max_per_branch=per_branch_limit,
            )
        )
    return project_resource_limits


async def _persist_project_with_limits(
    session: SessionDep,
    organization: OrganizationDep,
    parameters: ProjectCreate,
    calculated_limits: dict[ResourceType, tuple[int, int]],
) -> Project:
    entity = Project(
        organization=organization,
        name=parameters.name,
        max_backups=parameters.max_backups,
    )
    session.add(entity)
    await session.flush()
    project_resource_limits = _resource_limits_from_limits(organization.id, entity.id, calculated_limits)
    if project_resource_limits:
        session.add_all(project_resource_limits)
    await _commit_project(session)
    await session.refresh(entity)
    await session.refresh(organization)
    return entity


async def _commit_project(session: SessionDep) -> None:
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise


@api.post(
    "/",
    name="organizations:projects:create",
    status_code=201,
    response_model=ProjectPublic | None,
    responses={
        201: {
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": _links,
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def create(
    session: SessionDep,
    request: Request,
    _credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    organization: OrganizationDep,
    parameters: ProjectCreate,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    await _validate_project_backup_budget(session, organization, parameters.max_backups)

    org_limits = await get_organization_resource_limits(session, organization.id)
    requested_project_limits_raw = parameters.project_limits.model_dump(exclude_unset=True, exclude_none=True)
    requested_per_branch_limits_raw = parameters.per_branch_limits.model_dump(exclude_unset=True, exclude_none=True)
    project_limits = _normalize_limits(requested_project_limits_raw, "project_limits")
    per_branch_limits = _normalize_limits(requested_per_branch_limits_raw, "per_branch_limits")
    consumed_project_limits = await _get_consumed_project_limits(session, organization.id, project_limits)
    calculated_limits = _calculate_project_limits(
        project_limits, per_branch_limits, org_limits, consumed_project_limits
    )
    entity = await _persist_project_with_limits(session, organization, parameters, calculated_limits)
    entity_url = url_path_for(
        request,
        "organizations:projects:detail",
        organization_id=organization.id,
        project_id=entity.id,
    )
    payload = (await _public(entity)).model_dump() if response == "full" else None

    return JSONResponse(
        content=payload,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{project_id}", tags=["project"])
instance_api.include_router(branch_module.api, prefix="/branches")


@instance_api.get(
    "/",
    name="organizations:projects:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(_organization: OrganizationDep, project: ProjectDep) -> ProjectPublic:
    return await _public(project)


@instance_api.put(
    "/",
    name="organizations:projects:update",
    status_code=204,
    responses={
        204: {
            "content": None,
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": _links,
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def update(
    request: Request,
    session: SessionDep,
    organization: OrganizationDep,
    project: ProjectDep,
    parameters: ProjectUpdate,
):
    update_values = parameters.model_dump(exclude_unset=True, exclude_none=True)
    if "max_backups" in update_values:
        project_id = await project.awaitable_attrs.id
        await _validate_project_backup_budget(
            session,
            organization,
            update_values["max_backups"],
            exclude_project_id=project_id,
        )

    for key, value in update_values.items():
        assert hasattr(project, key)
        setattr(project, key, value)
    try:
        await session.commit()
    except IntegrityError as e:
        raise HTTPException(409, f"Organization already has project named {parameters.name}") from e

    # Refer to potentially updated location
    return Response(
        status_code=204,
        headers={
            "Location": url_path_for(
                request,
                "organizations:projects:detail",
                organization_id=await organization.awaitable_attrs.id,
                project_id=await project.awaitable_attrs.id,
            ),
        },
    )


@instance_api.delete(
    "/",
    name="organizations:projects:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(session: SessionDep, _organization: OrganizationDep, project: ProjectDep):
    project.status = "DELETING"
    await session.commit()

    await session.refresh(project, ["branches"])
    branches = await project.awaitable_attrs.branches
    try:
        for branch in branches:
            await delete_deployment(branch.id)
    except Exception:
        project.status = "ERROR"
        await session.commit()
        raise
    for branch in branches:
        await delete_branch_provisioning(session, branch, commit=False)

    await session.delete(project)
    await session.commit()
    return Response(status_code=204)


@instance_api.post(
    "/suspend",
    name="organizations:projects:suspend",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def suspend(session: SessionDep, _organization: OrganizationDep, project: ProjectDep):
    # get all the branches and stop their VM
    project.status = "PAUSING"
    await session.commit()

    branches = await project.awaitable_attrs.branches
    errors = []

    for branch in branches:
        namespace, vmi_name = get_db_vmi_identity(branch.id)
        autoscaler_namespace, autoscaler_vm_name = get_autoscaler_vm_identity(branch.id)
        try:
            # a paused VM will still consume resources, so we stop it instead
            # https://kubevirt.io/user-guide/user_workloads/lifecycle/#pausing-and-unpausing-a-virtual-machine
            await call_kubevirt_subresource(namespace, vmi_name, "stop")
        except ApiException as e:
            errors.append(f"{vmi_name}: {e.status}")
        try:
            await set_virtualmachine_power_state(autoscaler_namespace, autoscaler_vm_name, "Stopped")
        except ApiException as e:
            errors.append(f"{autoscaler_vm_name}: {e.status}")
        except VelaKubernetesError as e:
            errors.append(f"{autoscaler_vm_name}: {e}")

    if errors:
        project.status = "ERROR"
        await session.commit()
        raise HTTPException(status_code=400, detail={"failed": errors})

    project.status = "PAUSED"
    await session.commit()
    return Response(status_code=204)


@instance_api.post(
    "/resume",
    name="organizations:projects:resume",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def resume(session: SessionDep, _organization: OrganizationDep, project: ProjectDep):
    # get all the branches and start their VM
    project.status = "STARTING"
    await session.commit()

    branches = await project.awaitable_attrs.branches
    errors = []

    for branch in branches:
        namespace, vmi_name = get_db_vmi_identity(branch.id)
        autoscaler_namespace, autoscaler_vm_name = get_autoscaler_vm_identity(branch.id)
        try:
            await call_kubevirt_subresource(namespace, vmi_name, "start")
        except ApiException as e:
            errors.append(f"{vmi_name}: {e.status}")
        try:
            await set_virtualmachine_power_state(autoscaler_namespace, autoscaler_vm_name, "Running")
        except ApiException as e:
            errors.append(f"{autoscaler_vm_name}: {e.status}")
        except VelaKubernetesError as e:
            errors.append(f"{autoscaler_vm_name}: {e}")

    if errors:
        project.status = "ERROR"
        await session.commit()
        raise HTTPException(status_code=400, detail={"failed": errors})

    project.status = "STARTED"
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
