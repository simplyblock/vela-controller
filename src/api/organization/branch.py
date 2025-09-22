from collections.abc import Sequence
from typing import Literal

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from slugify import slugify

from ...deployment import delete_deployment
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..db import SessionDep
from ..models.branch import Branch, BranchCreate, BranchDep, BranchPublic, BranchUpdate
from ..models.organization import OrganizationDep
from ..models.project import ProjectDep

api = APIRouter()


async def _public(branch: Branch) -> BranchPublic:
    parent = await branch.awaitable_attrs.parent
    return BranchPublic(
        id=branch.dbid(),
        slug=branch.slug,
        name=branch.name,
        parent_slug=parent.slug if parent else None,
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
            "branch_slug": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "update": {
        "operationId": "organizations:projects:branch:update",
        "parameters": {
            "project_slug": "$response.header.Location#regex:/projects/(.+)/",
            "branch_slug": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "delete": {
        "operationId": "organizations:projects:branch:delete",
        "parameters": {
            "project_slug": "$response.header.Location#regex:/projects/(.+)/",
            "branch_slug": "$response.header.Location#regex:/branches/(.+)/",
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
    public_entity = BranchPublic(id=0, slug=branch_slug, name=parameters.name, parent_slug=None)
    entity_url = url_path_for(
        request,
        "organizations:projects:branch:detail",
        organization_slug=organization_slug,
        project_slug=project_slug,
        branch_slug=branch_slug,
    )
    # TODO: implement branch logic using clones
    return JSONResponse(
        content=public_entity.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{branch_slug}")


@instance_api.get(
    "/",
    name="organizations:projects:branch:detail",
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
    request: Request,
    _session: SessionDep,
    organization: OrganizationDep,
    project: ProjectDep,
    branch: BranchDep,
    _parameters: BranchUpdate,
):
    organization_slug = await organization.awaitable_attrs.slug
    project_slug = await project.awaitable_attrs.slug
    branch_slug = await branch.awaitable_attrs.slug
    # todo implemente update logic
    return Response(
        status_code=204,
        headers={
            "Location": url_path_for(
                request,
                "organizations:projects:branch:detail",
                organization_slug=organization_slug,
                project_slug=project_slug,
                branch_slug=branch_slug,
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
    delete_deployment(branch.project_id or branch.dbid(), branch.slug)
    await session.delete(branch)
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
