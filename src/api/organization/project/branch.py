from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic.config import ConfigDict
from slugify import slugify
from sqlmodel import Field

from ...._util import Slug
from ....deployment import delete_deployment
from ..._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...db import SessionDep
from ...models.branch import (
    Branch,
    BranchCreate,
    BranchDep,
    BranchDetailOptions,
    BranchDetailResources,
    BranchPublic,
    BranchUpdate,
)
from ...models.organization import OrganizationDep
from ...models.project import ProjectDep

api = APIRouter()


class BranchResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: Slug
    id: str
    project_id: str = Field(alias="project-id")
    meta_endpoint: str | None = None
    rest_endpoint: str | None = None
    options: BranchDetailOptions
    resources: BranchDetailResources
    created_at: datetime


async def _public(branch: Branch) -> BranchPublic:
    _ = await branch.awaitable_attrs.parent
    return BranchPublic(
        id=branch.dbid(),
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
            "project_slug": "$response.header.Location#regex:/projects/(.+)/",
            "branch": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "update": {
        "operationId": "organizations:projects:branch:update",
        "parameters": {
            "project_slug": "$response.header.Location#regex:/projects/(.+)/",
            "branch": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "delete": {
        "operationId": "organizations:projects:branch:delete",
        "parameters": {
            "project_slug": "$response.header.Location#regex:/projects/(.+)/",
            "branch": "$response.header.Location#regex:/branches/(.+)/",
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
    _session: SessionDep,
    request: Request,
    organization: OrganizationDep,
    project: ProjectDep,
    parameters: BranchCreate,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    organization_slug = await organization.awaitable_attrs.slug
    project_slug = await project.awaitable_attrs.slug
    branch_slug = slugify(parameters.name, max_length=50)
    public_entity = BranchPublic(id=0, name=parameters.name)
    entity_url = url_path_for(
        request,
        "organizations:projects:branch:detail",
        organization_slug=organization_slug,
        project_slug=project_slug,
        branch=branch_slug,
    )
    # TODO: implement branch logic using clones
    return JSONResponse(
        content=public_entity.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{branch}")


@instance_api.get(
    "/",
    name="organizations:projects:branch:detail",
    response_model=BranchResponse,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(
    _organization: OrganizationDep,
    project: ProjectDep,
    branch: BranchDep,
) -> BranchResponse:
    project_obj = project or await branch.awaitable_attrs.project
    domain = branch.endpoint_domain
    storage_gb = branch.database_size // (2**30)
    resources = BranchDetailResources(
        vcpu=branch.vcpu,
        ram_mb=branch.memory // (1024**2),
        nvme_gb=storage_gb,
        iops=branch.iops,
        storage_gb=storage_gb,
    )
    options = BranchDetailOptions(storage=branch.database_size > 0)

    meta_endpoint = f"https://{domain}/meta" if domain else None
    rest_endpoint = f"https://{domain}/rest" if domain else None

    created_at = branch.created_at or datetime.now(UTC)

    return BranchResponse(
        name=branch.name,
        id=branch.external_id or str(branch.dbid()),
        project_id=project_obj.external_id or str(project_obj.dbid()),
        meta_endpoint=meta_endpoint,
        rest_endpoint=rest_endpoint,
        options=options,
        resources=resources,
        created_at=created_at,
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
    request: Request,
    _session: SessionDep,
    organization: OrganizationDep,
    project: ProjectDep,
    branch: BranchDep,
    _parameters: BranchUpdate,
):
    organization_slug = await organization.awaitable_attrs.slug
    project_slug = await project.awaitable_attrs.slug
    branch_slug = await branch.awaitable_attrs.name
    # TODO implement update logic
    return Response(
        status_code=204,
        headers={
            "Location": url_path_for(
                request,
                "organizations:projects:branch:detail",
                organization_slug=organization_slug,
                project_slug=project_slug,
                branch=branch_slug,
            ),
        },
    )


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
    delete_deployment(branch.project_id or branch.dbid(), branch.name)
    await session.delete(branch)
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
