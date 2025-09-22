import asyncio
import base64
from collections.abc import Sequence
from typing import Literal

from Crypto.Cipher import AES
from Crypto.Hash import MD5
from Crypto.Random import get_random_bytes
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from kubernetes.client.exceptions import ApiException
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ...deployment import (
    create_vela_config,
    delete_deployment,
    get_db_vmi_identity,
    get_deployment_status,
)
from ...deployment.kubevirt import call_kubevirt_subresource
from ...deployment import DeploymentParameters, create_branch_deployment
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..db import SessionDep
from ..models.organization import OrganizationDep
from ..models.project import Project, ProjectCreate, ProjectDep, ProjectPublic, ProjectUpdate
from ..models.branch import Branch, BranchCreate, BranchPublic, BranchUpdate, BranchDep
from ..settings import settings

api = APIRouter()


def _evp_bytes_to_key(passphrase, salt) -> tuple[bytes, bytes]:
    d = d_i = b""
    while len(d) < 48:  # 32 bytes key + 16 bytes IV
        d_i = MD5.new(d_i + passphrase.encode("utf-8") + salt).digest()
        d += d_i

    return d[:32], d[32:48]


def _encrypt(plaintext, passphrase) -> str:
    salt = get_random_bytes(8)
    key, iv = _evp_bytes_to_key(passphrase, salt)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encoded = plaintext.encode("utf-8")
    padded = encoded + bytes([16 - len(encoded) % 16]) * (16 - len(encoded) % 16)
    return base64.b64encode(b"Salted__" + salt + cipher.encrypt(padded)).decode("utf-8")


def _public(project: Project) -> ProjectPublic:
    status = get_deployment_status(project.dbid())
    connection_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(  # noqa: UP032
        user=project.database_user,
        password=project.database_password,
        host="",  # FIXME Determine based on deployment
        port=5432,
        database=project.database,
    )
    return ProjectPublic(
        organization_id=project.db_org_id(),
        id=project.dbid(),
        slug=project.slug,
        name=project.name,
        status=status.status,
        deployment_status=(status.message, status.pods),
        database_user=project.database_user,
        encrypted_database_connection_string=_encrypt(connection_string, settings.pgmeta_crypto_key),
    )


@api.get(
    "/",
    name="organizations:projects:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_(session: SessionDep, organization: OrganizationDep) -> Sequence[ProjectPublic]:
    await session.refresh(organization, ["projects"])
    return [_public(project) for project in await organization.awaitable_attrs.projects]


_links = {
    "detail": {
        "operationId": "organizations:projects:detail",
        "parameters": {"project_slug": "$response.header.Location#regex:/projects/(.+)/"},
    },
    "update": {
        "operationId": "organizations:projects:update",
        "parameters": {"project_slug": "$response.header.Location#regex:/projects/(.+)/"},
    },
    "delete": {
        "operationId": "organizations:projects:delete",
        "parameters": {"project_slug": "$response.header.Location#regex:/projects/(.+)/"},
    },
}


@api.post(
    "/",
    name="organizations:projects:create",
    status_code=201,
    response_model=ProjectPublic | None,
    responses={
        201: {
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
        database=parameters.deployment.database,
        database_user=parameters.deployment.database_user,
        database_password=parameters.deployment.database_user,
        database_size=parameters.deployment.database_size,
        vcpu=parameters.deployment.vcpu,
        memory=parameters.deployment.memory,
        iops=parameters.deployment.iops,
        database_image_tag=parameters.deployment.database_image_tag,
    )
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as e:
        error = str(e)
        if ("asyncpg.exceptions.UniqueViolationError" not in error) or ("unique_project_slug" not in error):
            raise
        raise HTTPException(409, f"Organization already has project named {parameters.name}") from e
    await session.refresh(entity)
    # Ensure default branch `main` exists
    main_branch = Branch(name="main", project=entity, parent=None)
    session.add(main_branch)
    await session.commit()
    await session.refresh(main_branch)
    asyncio.create_task(create_vela_config(entity.dbid(), parameters.deployment))
    await session.refresh(organization)
    entity_url = url_path_for(
        request,
        "organizations:projects:detail",
        organization_slug=organization.id,
        project_slug=entity.slug,
    )
    return JSONResponse(
        content=_public(entity).model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{project_slug}")


@instance_api.get(
    "/",
    name="organizations:projects:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(_organization: OrganizationDep, project: ProjectDep) -> ProjectPublic:
    return _public(project)


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
                organization_slug=await organization.awaitable_attrs.id,
                project_slug=await project.awaitable_attrs.slug,
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
    delete_deployment(project.dbid())
    await session.delete(project)
    await session.commit()
    return Response(status_code=204)


@instance_api.post(
    "/pause",
    name="organizations:projects:pause",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def pause(_organization: OrganizationDep, project: ProjectDep):
    namespace, vmi_name = get_db_vmi_identity(project.dbid())
    try:
        call_kubevirt_subresource(namespace, vmi_name, "pause")
        return Response(status_code=204)
    except ApiException as e:
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e


@instance_api.post(
    "/resume",
    name="organizations:projects:resume",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def resume(_organization: OrganizationDep, project: ProjectDep):
    namespace, vmi_name = get_db_vmi_identity(project.dbid())
    try:
        call_kubevirt_subresource(namespace, vmi_name, "resume")
        return Response(status_code=204)
    except ApiException as e:
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e


api.include_router(instance_api)


# Branches API under a project
branches_api = APIRouter(prefix="/{project_slug}/branches")


def _branch_public(b, parent_slug: str | None) -> BranchPublic:
    return BranchPublic(id=b.id, slug=b.slug, name=b.name, parent_slug=parent_slug)


@branches_api.get(
    "/",
    name="organizations:projects:branches:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def branches_list(session: SessionDep, _organization: OrganizationDep, project: ProjectDep) -> list[BranchPublic]:
    await session.refresh(project, ["branches"])
    items = await project.awaitable_attrs.branches
    id_to_slug = {b.id: b.slug for b in items}
    return [
        _branch_public(b, id_to_slug.get(b.parent_id) if b.parent_id else None)
        for b in items
    ]


@branches_api.get(
    "/tree",
    name="organizations:projects:branches:tree",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def branches_tree(session: SessionDep, _organization: OrganizationDep, project: ProjectDep) -> list[dict]:
    await session.refresh(project, ["branches"])
    items = await project.awaitable_attrs.branches
    children: dict[int | None, list[int]] = {}
    by_id: dict[int, Branch] = {}
    for b in items:
        by_id[b.id] = b
        children.setdefault(b.parent_id, []).append(b.id)

    def build(node_id: int) -> dict:
        node = by_id[node_id]
        return {
            "id": node.id,
            "slug": node.slug,
            "name": node.name,
            "children": [build(cid) for cid in children.get(node_id, [])],
        }

    return [build(root_id) for root_id in children.get(None, [])]


@branches_api.get(
    "/{branch_slug}",
    name="organizations:projects:branches:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def branches_detail(_organization: OrganizationDep, project: ProjectDep, branch: BranchDep) -> BranchPublic:
    parent_slug = None
    if branch.parent_id:
        for b in await project.awaitable_attrs.branches:
            if b.id == branch.parent_id:
                parent_slug = b.slug
                break
    return _branch_public(branch, parent_slug)


@branches_api.put(
    "/{branch_slug}",
    name="organizations:projects:branches:update",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def branches_update(
    request: Request,
    session: SessionDep,
    organization: OrganizationDep,
    project: ProjectDep,
    branch: BranchDep,
    parameters: BranchUpdate,
):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert hasattr(branch, key)
        setattr(branch, key, value)
    await session.commit()

    return Response(
        status_code=204,
        headers={
            "Location": url_path_for(
                request,
                "organizations:projects:branches:detail",
                organization_slug=await organization.awaitable_attrs.id,
                project_slug=await project.awaitable_attrs.slug,
                branch_slug=await branch.awaitable_attrs.slug,
            ),
        },
    )


@branches_api.post(
    "/",
    name="organizations:projects:branches:create",
    status_code=201,
    response_model=BranchPublic | None,
    responses={
        201: {
            "content": None,
            "headers": {"Location": {"description": "URL of the created branch", "schema": {"type": "string"}}},
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def branches_create(
    session: SessionDep,
    request: Request,
    organization: OrganizationDep,
    project: ProjectDep,
    parameters: BranchCreate,
    response: Literal["empty", "full"] = "empty",
):
    # Resolve parent/source if provided
    parent_branch: Branch | None = None
    parent_id: int | None = None
    if parameters.source is not None:
        try:
            query = select(Branch).where(Branch.project_id == project.id, Branch.slug == parameters.source)
            parent_branch = (await session.exec(query)).one()
            parent_id = parent_branch.id
        except Exception as e:  # noqa: BLE001
            raise HTTPException(404, f"Source branch {parameters.source} not found") from e

    entity = Branch(name=parameters.name, project=project, parent_id=parent_id)
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as e:  # noqa: F841
        raise HTTPException(409, f"Project already has branch named {parameters.name}") from e
    await session.refresh(entity)

    # Build deployment parameters from project
    dep_params = DeploymentParameters(
        database=project.database,
        database_user=project.database_user,
        database_password=project.database_password,
        database_size=project.database_size,
        vcpu=project.vcpu,
        memory=project.memory,
        iops=project.iops,
        database_image_tag=project.database_image_tag,  # type: ignore[arg-type]
    )
    asyncio.create_task(
        create_branch_deployment(
            project_id=project.dbid(),
            parameters=dep_params,
            branch_id=entity.id,  # type: ignore[arg-type]
            clone_from_branch_id=parent_branch.id if (parameters.data_copy and parent_branch) else None,  # type: ignore[arg-type]
            data_copy=parameters.data_copy,
        )
    )

    entity_url = url_path_for(
        request,
        "organizations:projects:branches:detail",
        organization_slug=organization.id,
        project_slug=project.slug,
        branch_slug=entity.slug,
    )
    return JSONResponse(
        content=_branch_public(entity, parent_branch.slug if parent_branch else None).model_dump()
        if response == "full"
        else None,
        status_code=201,
        headers={"Location": entity_url},
    )


api.include_router(branches_api)
