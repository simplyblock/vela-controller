import base64
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Literal

from Crypto.Cipher import AES
from Crypto.Hash import MD5
from Crypto.Random import get_random_bytes
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from ...._util import Identifier
from ....deployment import (
    ResizeParameters,
    branch_domain,
    branch_rest_endpoint,
    delete_deployment,
    resize_deployment,
)
from ....deployment.settings import settings as deployment_settings
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...db import SessionDep
from ...models.branch import (
    Branch,
    BranchCreate,
    BranchDep,
    BranchPublic,
    BranchUpdate,
    DatabaseInformation,
    ResourcesDefinition,
)
from ...models.branch import lookup as lookup_branch
from ...models.organization import OrganizationDep
from ...models.project import ProjectDep
from ...settings import settings

api = APIRouter()


def _ulid_datetime_iso(value: Identifier) -> str:
    timestamp = getattr(value, "datetime", None)
    if timestamp is None:
        return datetime.now(UTC).isoformat()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.isoformat()


async def _public(branch: Branch) -> BranchPublic:
    project = await branch.awaitable_attrs.project

    host = branch.endpoint_domain or branch_domain(branch.id) or deployment_settings.deployment_host
    port = 5432

    connection_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(  # noqa: UP032
        user=branch.database_user,
        password=branch.database_password,
        host=host,
        port=port,
        database=branch.database,
    )

    rest_endpoint = branch_rest_endpoint(branch.id)
    service_host = host.replace("-db.", ".", 1)
    if rest_endpoint:
        service_endpoint = rest_endpoint.removesuffix("/rest")
        service_endpoint = service_endpoint.replace("-db.", ".", 1)
    else:
        service_endpoint = f"https://{service_host}"

    iops = max(branch.iops or 0, 100)

    max_resources = ResourcesDefinition(
        vcpu=branch.vcpu,
        ram_bytes=branch.memory,
        nvme_bytes=branch.database_size,
        iops=iops,
        storage_bytes=branch.database_size,
    )

    database_info = DatabaseInformation(
        host=host,
        port=port,
        username=branch.database_user,
        name=branch.database,
        encrypted_connection_string=_encrypt(connection_string, settings.pgmeta_crypto_key),
        service_endpoint_uri=service_endpoint,
        version=branch.database_image_tag,
        has_replicas=False,
    )

    return BranchPublic(
        id=branch.id,
        name=branch.name,
        project_id=branch.project_id,
        organization_id=project.organization_id,
        database=database_info,
        max_resources=max_resources,
        created_at=_ulid_datetime_iso(branch.id),
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
    # TODO implement cloning logic
    source = await lookup_branch(session, project, parameters.source)
    entity = Branch(
        name=parameters.name,
        project_id=project.id,
        parent_id=source.id,
        database=source.database,
        database_user=source.database_user,
        database_password=source.database_password,
        database_size=source.database_size,
        vcpu=source.vcpu,
        memory=source.memory,
        iops=max(source.iops or 0, 100),
        database_image_tag=source.database_image_tag,
    )
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        error = str(exc)
        if "asyncpg.exceptions.UniqueViolationError" in error and "unique_branch_name_per_project" in error:
            raise HTTPException(409, f"Project already has branch named {parameters.name}") from exc
        raise
    await session.refresh(entity)

    entity_url = url_path_for(
        request,
        "organizations:projects:branch:detail",
        organization_id=await organization.awaitable_attrs.id,
        project_id=await project.awaitable_attrs.id,
        branch_id=entity.id,
    )
    # TODO: implement branch logic using clones
    return JSONResponse(
        content=(await _public(entity)).model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{branch_id}")


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
    await delete_deployment(branch.project_id or branch.id, branch.name)
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
    resize_deployment(branch.id, branch.name, parameters)
    return Response(status_code=202)


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
