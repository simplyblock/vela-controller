import asyncio
from collections.abc import Sequence
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from ...deployment import create_vela_config, delete_deployment, get_deployment_status
from .._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ..db import SessionDep
from ..models.organization import OrganizationDep
from ..models.project import Project, ProjectCreate, ProjectDep, ProjectPublic, ProjectUpdate

api = APIRouter()


def _public(project: Project) -> ProjectPublic:
    status = get_deployment_status(project.dbid())
    return ProjectPublic(
            organization_id=project.db_org_id(),
            id=project.dbid(),
            name=project.name,
            status=status.status,
            deployment_status=(status.message, status.pods),
    )


@api.get(
        '/', name='organizations:projects:list',
        responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_(session: SessionDep, organization: OrganizationDep) -> Sequence[ProjectPublic]:
    await session.refresh(organization, ['projects'])
    return [_public(project) for project in await organization.awaitable_attrs.projects]


_links = {
    'detail': {
        'operationId': 'organizations:projects:detail',
        'parameters': {'project_slug': '$response.header.Location#regex:/projects/(.+)/'},
    },
    'update': {
        'operationId': 'organizations:projects:update',
        'parameters': {'project_slug': '$response.header.Location#regex:/projects/(.+)/'},
    },
    'delete': {
        'operationId': 'organizations:projects:delete',
        'parameters': {'project_slug': '$response.header.Location#regex:/projects/(.+)/'},
    },
}


@api.post(
        '/', name='organizations:projects:create', status_code=201,
        response_model=ProjectPublic | None,
        responses={
            201: {
                'content': None,
                'headers': {
                    'Location': {
                        'description': 'URL of the created item',
                        'schema': {'type': 'string'},
                    },
                },
                'links': _links,
            },
            401: Unauthenticated,
            403: Forbidden,
            404: NotFound,
            409: Conflict,
        },
)
async def create(
        session: SessionDep, request: Request,
        organization: OrganizationDep, parameters: ProjectCreate,
        response: Literal['empty', 'full'] = 'empty',
) -> JSONResponse:
    entity = Project(
            organization=organization,
            name=parameters.name,
            database=parameters.deployment.database,
            database_user=parameters.deployment.database_user,
    )
    session.add(entity)
    try:
        await session.commit()
    except IntegrityError as e:
        raise HTTPException(409, f'Organization already has project named {parameters.name}') from e
    await session.refresh(entity)
    asyncio.create_task(create_vela_config(entity.dbid(), parameters.deployment))
    await session.refresh(organization)
    entity_url = url_path_for(
            request, 'organizations:projects:detail',
            organization_slug=organization.id, project_slug=entity.slug,
    )
    return JSONResponse(
            content=_public(entity).model_dump() if response == 'full' else None,
            status_code=201,
            headers={'Location': entity_url},
    )


instance_api = APIRouter(prefix='/{project_slug}')


@instance_api.get(
        '/', name='organizations:projects:detail',
        responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(_organization: OrganizationDep, project: ProjectDep) -> ProjectPublic:
    return _public(project)


@instance_api.put(
        '/', name='organizations:projects:update',
        status_code=204,
        responses={
            204: {
                'content': None,
                'headers': {
                    'Location': {
                        'description': 'URL of the created item',
                        'schema': {'type': 'string'},
                    },
                },
                'links': _links,
            },
            401: Unauthenticated, 403: Forbidden, 404: NotFound, 409: Conflict,
        },
)
async def update(
        request: Request, session: SessionDep,
        organization: OrganizationDep, project:
        ProjectDep, parameters: ProjectUpdate,
):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert(hasattr(project, key))
        setattr(project, key, value)
    try:
        await session.commit()
    except IntegrityError as e:
        raise HTTPException(409, f'Organization already has project named {parameters.name}') from e

    # Refer to potentially updated location
    return Response(status_code=204, headers={
            'Location': url_path_for(
                request, 'organizations:projects:detail',
                organization_slug=await organization.awaitable_attrs.id,
                project_slug=await project.awaitable_attrs.slug,
            ),
    })


@instance_api.delete(
        '/', name='organizations:projects:delete',
        status_code=204,
        responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(session: SessionDep, _organization: OrganizationDep, project: ProjectDep):
    delete_deployment(project.dbid())
    await session.delete(project)
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
