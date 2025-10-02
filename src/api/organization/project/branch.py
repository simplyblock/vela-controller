from collections.abc import Sequence
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ...._util import Slug
from ....deployment import (
    ResizeParameters,
    apply_vm_runtime_resources,
    branch_rest_endpoint,
    delete_deployment,
    resize_deployment,
)
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...db import SessionDep
from ...models.branch import (
    Branch,
    BranchCreate,
    BranchDep,
    BranchDetailResources,
    BranchPublic,
    BranchUpdate,
    gib_decimal_to_bytes,
)
from ...models.branch import lookup as lookup_branch
from ...models.organization import OrganizationDep
from ...models.project import ProjectDep

api = APIRouter()


class BranchResponse(BaseModel):
    name: Slug
    id: str
    rest_endpoint: str | None = None
    resources: BranchDetailResources


async def _public(branch: Branch) -> BranchPublic:
    _ = await branch.awaitable_attrs.parent
    return BranchPublic(
        id=branch.id,
        name=branch.name,
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
        database_size=source.database_size,
        vcpu=source.vcpu,
        memory=source.memory,
        iops=source.iops,
        database_image_tag=source.database_image_tag,
    )
    session.add(entity)
    await session.commit()
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
    response_model=BranchResponse,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> BranchResponse:
    resources = BranchDetailResources(
        vcpu=branch.vcpu,
        ram_gib=branch.memory,
        ram_bytes=gib_decimal_to_bytes(branch.memory),
        nvme_bytes=branch.database_size,
        iops=branch.iops,
        storage_bytes=branch.database_size,
    )
    rest_endpoint = branch_rest_endpoint(branch.id)

    return BranchResponse(
        name=branch.name,
        id=str(branch.id),
        rest_endpoint=rest_endpoint,
        resources=resources,
    )


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
async def resize(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    parameters: ResizeParameters,
    branch: BranchDep,
):
    updates = parameters.model_dump(exclude_unset=True, exclude_none=True)

    if (database_size := updates.get("database_size")) is not None:
        resize_deployment(branch.id, branch.name, parameters)
        branch.database_size = database_size

    cpu_target = updates.get("vcpu")
    memory_target = updates.get("memory")

    if cpu_target is not None:
        branch.vcpu = cpu_target
    if memory_target is not None:
        branch.memory = memory_target

    if (cpu_target is not None) or (memory_target is not None):
        await apply_vm_runtime_resources(branch.project_id, branch.name, cpu=cpu_target, memory=memory_target)

    if updates:
        await session.commit()
    return Response(status_code=202)


api.include_router(instance_api)
