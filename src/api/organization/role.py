from collections.abc import Sequence
from typing import Literal

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from .._util import Forbidden, NotFound, Unauthenticated, url_path_for
from ..auth import UserDep
from ..db import SessionDep
from ..models.organization import OrganizationDep
from ..models.role import Role, RoleDep

api = APIRouter()


@api.get(
    "/",
    name="organizations:roles:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_(organization: OrganizationDep) -> Sequence[Role]:
    return await organization.awaitable_attrs.roles


@api.post(
    "/",
    name="organizations:roles:create",
    status_code=201,
    response_model=Role | None,
    responses={
        201: {
            "content": None,
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": {
                "detail": {
                    "operationId": "organizations:roles:detail",
                    "parameters": {"role_id": "$response.header.Location#regex:/roles/(.+)/"},
                },
                "update": {
                    "operationId": "organizations:roles:update",
                    "parameters": {"role_id": "$response.header.Location#regex:/roles/(.+)/"},
                },
                "delete": {
                    "operationId": "organizations:roles:delete",
                    "parameters": {"role_id": "$response.header.Location#regex:/roles/(.+)/"},
                },
            },
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
    },
)
async def create(
    session: SessionDep,
    request: Request,
    organization: OrganizationDep,
    user: UserDep,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    entity = Role(organization=organization, user=user)
    session.add(entity)
    await session.commit()
    await session.refresh(entity)
    entity_url = url_path_for(
        request,
        "organizations:roles:detail",
        organization_slug=await organization.awaitable_attrs.slug,
        role_id=entity.dbid(),
    )
    return JSONResponse(
        content=entity.model_dump() if response == "full" else None,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{role_id}")


@instance_api.get(
    "/",
    name="organizations:roles:detail",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(_organization: OrganizationDep, role: RoleDep) -> Role:
    return role


@instance_api.put(
    "/",
    name="organizations:roles:update",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update(_organization: OrganizationDep, _role: RoleDep):
    return Response(content="", status_code=204)


@instance_api.delete(
    "/",
    name="organizations:roles:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(session: SessionDep, _organization: OrganizationDep, role: RoleDep):
    await session.delete(role)
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
