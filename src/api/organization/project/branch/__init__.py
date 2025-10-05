import asyncio
import base64
import logging
from collections.abc import Sequence
from typing import Any, Literal, cast

from Crypto.Cipher import AES
from Crypto.Hash import MD5
from Crypto.Random import get_random_bytes
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from keycloak.exceptions import KeycloakError
from kubernetes_asyncio.client.exceptions import ApiException
from sqlalchemy.exc import IntegrityError

from ....._util import Identifier
from .....deployment import (
    DeploymentParameters,
    ResizeParameters,
    branch_api_domain,
    branch_domain,
    branch_rest_endpoint,
    delete_deployment,
    deploy_branch_environment,
    get_db_vmi_identity,
    resize_deployment,
)
from .....deployment.kubevirt import KubevirtSubresourceAction, call_kubevirt_subresource, get_virtualmachine_status
from .....deployment.settings import settings as deployment_settings
from .....exceptions import VelaDeploymentError, VelaError
from ...._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ....db import SessionDep
from ....keycloak import realm_admin
from ....models.branch import (
    Branch,
    BranchApiKeys,
    BranchCreate,
    BranchDep,
    BranchPublic,
    BranchStatus,
    BranchUpdate,
    DatabaseInformation,
    ResourceUsageDefinition,
)
from ....models.branch import (
    lookup as lookup_branch,
)
from ....models.organization import OrganizationDep
from ....models.project import ProjectDep
from ....settings import settings
from .auth import api as auth_api

api = APIRouter(tags=["branch"])


async def _deploy_branch_environment_task(
    *,
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: str,
    parameters: DeploymentParameters,
) -> None:
    try:
        await deploy_branch_environment(
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            parameters=parameters,
        )
    except VelaError:
        logging.exception(
            "Branch deployment failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )


async def _public(branch: Branch) -> BranchPublic:
    project = await branch.awaitable_attrs.project

    db_host = branch.endpoint_domain or branch_domain(branch.id)
    if not db_host:
        db_host = deployment_settings.deployment_host
    port = 5432

    connection_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(  # noqa: UP032
        user=branch.database_user,
        password=branch.database_password,
        host=db_host,
        port=port,
        database=branch.database,
    )

    rest_endpoint = branch_rest_endpoint(branch.id)
    api_domain = branch_api_domain(branch.id)

    if rest_endpoint:
        service_endpoint = rest_endpoint.removesuffix("/rest")
    elif api_domain:
        service_endpoint = f"https://{api_domain}"
    else:
        # Fall back to using the same host as the database when dedicated domains are unavailable.
        service_endpoint = f"https://{db_host}"

    max_resources = branch.provisioned_resources()

    database_info = DatabaseInformation(
        host=db_host,
        port=port,
        username=branch.database_user,
        name=branch.database,
        encrypted_connection_string=_encrypt(connection_string, settings.pgmeta_crypto_key),
        service_endpoint_uri=service_endpoint,
        version=branch.database_image_tag,
        has_replicas=False,
    )

    # FIXME: Replace placeholder telemetry data once usage metrics and labels are wired in.
    used_resources = ResourceUsageDefinition(
        milli_vcpu=0,
        ram_bytes=0,
        nvme_bytes=0,
        iops=0,
        storage_bytes=None,
    )
    namespace, vmi_name = get_db_vmi_identity(branch.id)
    try:
        status = await get_virtualmachine_status(namespace, vmi_name)
        # TODO: replace with real service health status once available
        _service_health = BranchStatus(
            database="ACTIVE_HEALTHY" if status == "Running" else "STOPPED",
            realtime="ACTIVE_HEALTHY" if status == "Running" else "STOPPED",
            storage="ACTIVE_HEALTHY" if status == "Running" else "STOPPED",
            meta="ACTIVE_HEALTHY" if status == "Running" else "STOPPED",
            rest="ACTIVE_HEALTHY" if status == "Running" else "STOPPED",
        )
    except VelaDeploymentError:
        logging.exception("Failed to query VM status")
        status = "UNKNOWN"
        _service_health = BranchStatus(
            database="UNKNOWN",
            realtime="UNKNOWN",
            storage="UNKNOWN",
            meta="UNKNOWN",
            rest="UNKNOWN",
        )

    api_keys = BranchApiKeys(anon="", service_role="")

    return BranchPublic(
        id=branch.id,
        name=branch.name,
        project_id=branch.project_id,
        organization_id=project.organization_id,
        database=database_info,
        max_resources=max_resources,
        assigned_labels=[],
        used_resources=used_resources,
        api_keys=api_keys,
        status=status,
        service_health=_service_health,
        ptir_enabled=False,
        created_at=branch.created_datetime,
        created_by="system",  # TODO: update it when user management is in place
        updated_at=None,
        updated_by=None,
    )


@api.get(
    "/",
    name="organizations:projects:branch:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_branches(
    session: SessionDep,
    _organization: OrganizationDep,
    project: ProjectDep,
) -> Sequence[BranchPublic]:
    await session.refresh(project, ["branches"])
    branches = await project.awaitable_attrs.branches
    return [await _public(branch) for branch in branches]


_links = {
    "detail": {
        "operationId": "organizations:projects:branch:detail",
        "parameters": {
            "project_id": "$response.header.Location#regex:/projects/(.+)/",
            "branch_id": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "update": {
        "operationId": "organizations:projects:branch:update",
        "parameters": {
            "project_id": "$response.header.Location#regex:/projects/(.+)/",
            "branch_id": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "delete": {
        "operationId": "organizations:projects:branch:delete",
        "parameters": {
            "project_id": "$response.header.Location#regex:/projects/(.+)/",
            "branch_id": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
}


@api.post(
    "/",
    name="organizations:projects:branch:create",
    status_code=201,
    response_model=BranchPublic | None,
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
    project: ProjectDep,
    parameters: BranchCreate,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    if parameters.source is not None:
        source = await lookup_branch(session, project, parameters.source.branch_id)
        entity = Branch(
            name=parameters.name,
            project_id=project.id,
            parent_id=source.id,
            database=source.database,
            database_user=source.database_user,
            database_password=source.database_password,
            database_size=source.database_size,
            storage_size=source.storage_size,
            milli_vcpu=source.milli_vcpu,
            memory=source.memory,
            iops=source.iops,
            database_image_tag=source.database_image_tag,
        )
    else:
        deployment_params = cast("DeploymentParameters", parameters.deployment)
        entity = Branch(
            name=parameters.name,
            project_id=project.id,
            parent=None,
            database=deployment_params.database,
            database_user=deployment_params.database_user,
            database_password=deployment_params.database_password,
            database_size=deployment_params.database_size,
            storage_size=deployment_params.storage_size,
            milli_vcpu=deployment_params.milli_vcpu,
            memory=deployment_params.memory_bytes,
            iops=deployment_params.iops,
            database_image_tag=deployment_params.database_image_tag,
        )
    session.add(entity)
    try:
        await realm_admin("master").a_create_realm({"realm": str(entity.id)})
        await realm_admin(str(entity.id)).a_create_client({"clientId": "application-client"})
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

    entity_url = url_path_for(
        request,
        "organizations:projects:branch:detail",
        organization_id=await organization.awaitable_attrs.id,
        project_id=await project.awaitable_attrs.id,
        branch_id=entity.id,
    )
    if parameters.deployment is not None:
        asyncio.create_task(
            _deploy_branch_environment_task(
                project_id=project.id,
                branch_id=entity.id,
                branch_slug=entity.name,
                parameters=parameters.deployment,
            )
        )
    # TODO: implement source branch cloning for config/data copy
    payload = (await _public(entity)).model_dump() if response == "full" else None

    return JSONResponse(
        content=payload,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{branch_id}", tags=["branch"])


@instance_api.get(
    "/",
    name="organizations:projects:branch:detail",
    response_model=BranchPublic,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> BranchPublic:
    return await _public(branch)


@instance_api.put(
    "/",
    name="organizations:projects:branch:update",
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
    _session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    _branch: BranchDep,
    _parameters: BranchUpdate,
):
    # TODO implement update logic
    return Response(status_code=204)


@instance_api.delete(
    "/",
    name="organizations:projects:branch:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
):
    if branch.name == Branch.DEFAULT_SLUG:
        raise HTTPException(400, "Default branch cannot be deleted")
    await delete_deployment(branch.id)
    await realm_admin("master").a_delete_realm(str(branch.id))
    await session.delete(branch)
    await session.commit()
    return Response(status_code=204)


# Resize controls
@instance_api.post(
    "/resize",
    name="organizations:projects:branch:resize",
    status_code=202,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def resize(_organization: OrganizationDep, _project: ProjectDep, parameters: ResizeParameters, branch: BranchDep):
    # Trigger helm upgrade with provided parameters; returns 202 Accepted
    resize_deployment(branch.id, parameters)
    return Response(status_code=202)


_control_responses: dict[int | str, dict[str, Any]] = {
    401: Unauthenticated,
    403: Forbidden,
    404: NotFound,
}

_CONTROL_TO_KUBEVIRT: dict[str, KubevirtSubresourceAction] = {
    "pause": "pause",
    "resume": "unpause",
    "start": "start",
    "stop": "stop",
}


@instance_api.post(
    "/pause",
    name="organizations:projects:branch:pause",
    status_code=204,
    responses=_control_responses,
)
@instance_api.post(
    "/resume",
    name="organizations:projects:branch:resume",
    status_code=204,
    responses=_control_responses,
)
@instance_api.post(
    "/start",
    name="organizations:projects:branch:start",
    status_code=204,
    responses=_control_responses,
)
@instance_api.post(
    "/stop",
    name="organizations:projects:branch:stop",
    status_code=204,
    responses=_control_responses,
)
async def control_branch(
    request: Request,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
):
    action = request.scope["route"].name.split(":")[-1]
    assert action in _CONTROL_TO_KUBEVIRT
    namespace, vmi_name = get_db_vmi_identity(branch.id)
    try:
        await call_kubevirt_subresource(namespace, vmi_name, _CONTROL_TO_KUBEVIRT[action])
        return Response(status_code=204)
    except ApiException as e:
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e


instance_api.include_router(auth_api, prefix="/auth")

api.include_router(instance_api)


def _evp_bytes_to_key(passphrase: str, salt: bytes) -> tuple[bytes, bytes]:
    d = d_i = b""
    while len(d) < 48:  # 32 bytes key + 16 bytes IV
        d_i = MD5.new(d_i + passphrase.encode("utf-8") + salt).digest()
        d += d_i

    return d[:32], d[32:48]


def _encrypt(plaintext: str, passphrase: str) -> str:
    salt = get_random_bytes(8)
    key, iv = _evp_bytes_to_key(passphrase, salt)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encoded = plaintext.encode("utf-8")
    padding = 16 - (len(encoded) % 16)
    padded = encoded + bytes([padding]) * padding
    payload = cipher.encrypt(padded)
    return base64.b64encode(b"Salted__" + salt + payload).decode("utf-8")
