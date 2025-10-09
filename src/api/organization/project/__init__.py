import asyncio
import logging
from collections.abc import Sequence
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.exc import IntegrityError

from ...._util import Identifier, StatusType, Name
from ....deployment import (
    DeploymentParameters,
    delete_deployment,
    deploy_branch_environment,
    get_db_vmi_identity,
    get_deployment_status,
)
from ....deployment.kubevirt import call_kubevirt_subresource
from ....exceptions import VelaError
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...db import SessionDep
from ...auth import AuthUserDep
from ...models.branch import Branch
from ...models.organization import OrganizationDep
from ...models.project import Project, ProjectCreate, ProjectDep, ProjectPublic, ProjectUpdate
from . import branch as branch_module

logger = logging.getLogger(__name__)

api = APIRouter()


async def _deploy_branch_environment_task(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: str,
    token: Any,
    parameters: DeploymentParameters,
) -> None:
    try:
        await deploy_branch_environment(
            organization_id=organization_id,
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            token=token,
            parameters=parameters,
        )
    except VelaError:
        logger.exception(
            "Branch deployment failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )


async def _public(project: Project) -> ProjectPublic:
    branch_status: dict[Any, StatusType] = {}
    branches = await project.awaitable_attrs.branches
    if not branches:
        raise HTTPException(500, "Project has no branches")
    for branch in branches:
        status = await get_deployment_status(branch.id)
        branch_status[branch.name] = status.status
    return ProjectPublic(
        organization_id=project.organization_id,
        id=project.id,
        name=project.name,
        branch_status=branch_status,
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
    organization: OrganizationDep,
    parameters: ProjectCreate,
    user: AuthUserDep,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    entity = Project(
        organization=organization,
        name=parameters.name,
    )
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as e:
        error = str(e)
        if ("asyncpg.exceptions.UniqueViolationError" not in error) or ("unique_project_name" not in error):
            raise
        raise HTTPException(409, f"Organization already has project named {parameters.name}") from e
    await session.refresh(entity)
    # Ensure default branch exists
    main_branch = Branch(
        name=Branch.DEFAULT_SLUG,
        project=entity,
        parent=None,
        database=parameters.deployment.database,
        database_user=parameters.deployment.database_user,
        database_password=parameters.deployment.database_password,
        database_size=parameters.deployment.database_size,
        storage_size=parameters.deployment.storage_size,
        milli_vcpu=parameters.deployment.milli_vcpu,
        memory=parameters.deployment.memory_bytes,
        iops=parameters.deployment.iops,
        database_image_tag=parameters.deployment.database_image_tag,
    )
    session.add(main_branch)
    await session.commit()
    await session.refresh(main_branch)
    await session.refresh(entity)
    branch_slug = main_branch.name
    branch_dbid = main_branch.id

    asyncio.create_task(
        _deploy_branch_environment_task(
            organization_id=entity.organization_id,
            project_id=entity.id,
            branch_id=branch_dbid,
            branch_slug=branch_slug,
            token=request,
            parameters=parameters.deployment,
        )
    )
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


instance_api = APIRouter(prefix="/{project_id}")
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
    await session.refresh(project, ["branches"])
    branches = await project.awaitable_attrs.branches
    for branch in branches:
        await delete_deployment(branch.id)
    await session.delete(project)
    await session.commit()
    return Response(status_code=204)


@instance_api.post(
    "/suspend",
    name="organizations:projects:suspend",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def suspend(_organization: OrganizationDep, project: ProjectDep):
    # get all the branches and stop their VM
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
        raise HTTPException(status_code=400, detail={"failed": errors})

    return Response(status_code=204)


@instance_api.post(
    "/resume",
    name="organizations:projects:resume",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def resume(_organization: OrganizationDep, project: ProjectDep):
    # get all the branches and start their VM
    branches = await project.awaitable_attrs.branches
    errors = []

    for branch in branches:
        namespace, vmi_name = get_db_vmi_identity(branch.id)
        try:
            await call_kubevirt_subresource(namespace, vmi_name, "start")
        except ApiException as e:
            errors.append(f"{vmi_name}: {e.status}")

    if errors:
        raise HTTPException(status_code=400, detail={"failed": errors})

    return Response(status_code=204)


api.include_router(instance_api)
