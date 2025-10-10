import asyncio
import logging
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Literal

import jwt
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from keycloak.exceptions import KeycloakError
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.exc import IntegrityError

from ...._util import Identifier
from ....deployment import (
    DeploymentParameters,
    delete_deployment,
    deploy_branch_environment,
    get_db_vmi_identity,
)
from ....deployment.kubevirt import call_kubevirt_subresource
from ....exceptions import VelaError
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...db import SessionDep
from ...keycloak import realm_admin
from ...models.branch import Branch
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
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
) -> None:
    try:
        await deploy_branch_environment(
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            parameters=parameters,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
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
        organization_id=project.organization_id,
        id=project.id,
        name=project.name,
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
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    entity = Project(
        organization=organization,
        name=parameters.name,
    )
    session.add(entity)
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
    try:
        await realm_admin("master").a_create_realm({"realm": str(main_branch.id)})
        await realm_admin(str(main_branch.id)).a_create_client({"clientId": "application-client"})
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        error = str(exc)
        if "asyncpg.exceptions.UniqueViolationError" in error and "unique_branch_name_per_project" in error:
            raise HTTPException(409, f"Project already has branch named {parameters.name}") from exc
        raise
    except KeycloakError:
        await session.rollback()
        logging.exception("Failed to connect to keycloak")
        raise

    await session.refresh(entity)
    await session.refresh(main_branch)
    project_id = entity.id
    branch_slug = main_branch.name
    branch_dbid = main_branch.id

    # Generate keys and store keys
    jwt_secret, anon_key, service_key = _generate_keys(branch_dbid.__str__())
    main_branch.jwt_secret = jwt_secret
    main_branch.anon_key = anon_key
    main_branch.service_key = service_key
    await session.commit()

    asyncio.create_task(
        _deploy_branch_environment_task(
            project_id=project_id,
            branch_id=branch_dbid,
            branch_slug=branch_slug,
            parameters=parameters.deployment,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
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


def _generate_keys(branch_id: str) -> tuple[str, str, str]:
    """Generates JWT secret, anon key, and service role key"""
    jwt_secret = secrets.token_urlsafe(32)

    iat = int(datetime.now(UTC).timestamp())
    # 10 years expiration
    exp = int((datetime.now(UTC) + timedelta(days=365 * 10)).timestamp())

    anon_payload = {
        "iss": "supabase",
        "ref": branch_id,
        "role": "anon",
        "iat": iat,
        "exp": exp,
    }

    service_role_payload = {
        "iss": "supabase",
        "ref": branch_id,
        "role": "service_role",
        "iat": iat,
        "exp": exp,
    }

    anon_key = jwt.encode(
        payload=anon_payload,
        key=jwt_secret,
        algorithm="HS256",
        headers={"alg": "HS256", "typ": "JWT"},
    )

    service_key = jwt.encode(
        payload=service_role_payload,
        key=jwt_secret,
        algorithm="HS256",
        headers={"alg": "HS256", "typ": "JWT"},
    )

    return jwt_secret, anon_key, service_key
