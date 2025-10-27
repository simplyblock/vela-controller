import asyncio
import logging
from collections.abc import Sequence
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from keycloak.exceptions import KeycloakError
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.exc import IntegrityError

from ...._util import DEFAULT_DB_NAME, DEFAULT_DB_USER, Identifier
from ....deployment import (
    DeploymentParameters,
    delete_deployment,
    deploy_branch_environment,
    get_db_vmi_identity,
)
from ....deployment.kubernetes.kubevirt import call_kubevirt_subresource
from ....exceptions import VelaError
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..._util.crypto import generate_keys
from ...auth import security
from ...db import SessionDep
from ...keycloak import realm_admin
from ...models.branch import Branch, PgbouncerConfig
from ...models.organization import OrganizationDep
from ...models.project import (
    Project,
    ProjectCreate,
    ProjectDep,
    ProjectPublic,
    ProjectUpdate,
)
from . import branch as branch_module

logger = logging.getLogger(__name__)

api = APIRouter(tags=["project"])


async def _deploy_branch_environment_task(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: str,
    credential: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    pgbouncer_config: branch_module.PgbouncerConfigSnapshot,
) -> None:
    try:
        await deploy_branch_environment(
            organization_id=organization_id,
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            credential=credential,
            parameters=parameters,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
            pgbouncer_admin_password=pgbouncer_admin_password,
            pgbouncer_config=branch_module.pgbouncer_snapshot_to_mapping(pgbouncer_config),
        )
    except VelaError:
        logger.exception(
            "Branch deployment failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )


async def _public(project: Project) -> ProjectPublic:
    return ProjectPublic(
        organization_id=await project.awaitable_attrs.organization_id,
        id=await project.awaitable_attrs.id,
        name=await project.awaitable_attrs.name,
        max_backups=await project.awaitable_attrs.max_backups,
        status="STARTED",  # TODO @Manohar please fill in the correct status
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
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
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
    main_branch = Branch(
        name=Branch.DEFAULT_SLUG,
        project=entity,
        parent=None,
        database=DEFAULT_DB_NAME,
        database_user=DEFAULT_DB_USER,
        database_size=parameters.deployment.database_size,
        storage_size=parameters.deployment.storage_size,
        milli_vcpu=parameters.deployment.milli_vcpu,
        memory=parameters.deployment.memory_bytes,
        iops=parameters.deployment.iops,
        database_image_tag=parameters.deployment.database_image_tag,
        env_type=parameters.env_type,
    )
    main_branch.database_password = parameters.deployment.database_password
    pgbouncer_admin_password = branch_module.generate_pgbouncer_password()
    main_branch.pgbouncer_password = pgbouncer_admin_password
    main_branch.pgbouncer_config = PgbouncerConfig(
        default_pool_size=PgbouncerConfig.DEFAULT_POOL_SIZE,
        max_client_conn=PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN,
        server_idle_timeout=PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT,
        server_lifetime=PgbouncerConfig.DEFAULT_SERVER_LIFETIME,
    )
    session.add(main_branch)
    try:
        await realm_admin("master").a_create_realm({"realm": str(main_branch.id)})
        await realm_admin(str(main_branch.id)).a_create_client({"clientId": "application-client"})
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        error = str(exc)
        if ("asyncpg.exceptions.UniqueViolationError" in error) and ("unique_project_name" in error):
            raise HTTPException(409, f"Organization already has project named {parameters.name}") from exc
        raise
    except KeycloakError:
        await session.rollback()
        logging.exception("Failed to connect to keycloak")
        raise

    await session.refresh(entity)
    await session.refresh(main_branch)
    pgbouncer_config_snapshot = branch_module.snapshot_pgbouncer_config(
        await main_branch.awaitable_attrs.pgbouncer_config
    )
    organization_id = entity.organization_id
    project_id = entity.id
    branch_slug = main_branch.name
    branch_dbid = main_branch.id

    # Generate keys and store keys
    jwt_secret, anon_key, service_key = generate_keys(branch_dbid.__str__())
    main_branch.jwt_secret = jwt_secret
    main_branch.anon_key = anon_key
    main_branch.service_key = service_key
    credential = credentials.credentials
    await session.commit()

    asyncio.create_task(
        _deploy_branch_environment_task(
            organization_id=organization_id,
            credential=credential,
            project_id=project_id,
            branch_id=branch_dbid,
            branch_slug=branch_slug,
            parameters=parameters.deployment,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
            pgbouncer_admin_password=pgbouncer_admin_password,
            pgbouncer_config=pgbouncer_config_snapshot,
        )
    )
    await session.refresh(organization)
    entity_url = url_path_for(
        request,
        "organizations:projects:detail",
        organization_id=organization.id,
        project_id=project_id,
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
