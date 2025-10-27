from collections.abc import Sequence
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.exc import IntegrityError

from ....deployment import delete_deployment, get_db_vmi_identity
from ....deployment.kubernetes.kubevirt import call_kubevirt_subresource
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...auth import security
from ...db import SessionDep
from ...models.organization import OrganizationDep
from ...models.project import (
    Project,
    ProjectCreate,
    ProjectDep,
    ProjectPublic,
    ProjectUpdate,
)
from . import branch as branch_module

api = APIRouter(tags=["project"])


async def _public(project: Project) -> ProjectPublic:
    return ProjectPublic(
        organization_id=await project.awaitable_attrs.organization_id,
        id=await project.awaitable_attrs.id,
        name=await project.awaitable_attrs.name,
        max_backups=await project.awaitable_attrs.max_backups,
        status=await project.awaitable_attrs.status,
        default_branch_id=None,  # TODO @Manohar please fill in the correct value
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
    entity = Project(
        organization=organization,
        name=parameters.name,
        max_backups=parameters.max_backups,
    )
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        error = str(exc)
        if ("asyncpg.exceptions.UniqueViolationError" in error) and ("unique_project_name" in error):
            raise HTTPException(409, f"Organization already has project named {parameters.name}") from exc
        raise

    await session.refresh(entity)
    await session.refresh(organization)
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
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
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
        try:
            # a paused VM will still consume resources, so we stop it instead
            # https://kubevirt.io/user-guide/user_workloads/lifecycle/#pausing-and-unpausing-a-virtual-machine
            await call_kubevirt_subresource(namespace, vmi_name, "stop")
        except ApiException as e:
            errors.append(f"{vmi_name}: {e.status}")

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
        try:
            await call_kubevirt_subresource(namespace, vmi_name, "start")
        except ApiException as e:
            errors.append(f"{vmi_name}: {e.status}")

    if errors:
        project.status = "ERROR"
        await session.commit()
        raise HTTPException(status_code=400, detail={"failed": errors})

    project.status = "STARTED"
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
