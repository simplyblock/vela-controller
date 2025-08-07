from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from ...db import SessionDep
from ...models.project import Project, ProjectCreate, ProjectUpdate
from .._util import Int64
from ..organization import OrganizationDep

api = APIRouter(prefix='/projects')


@api.get(
        '/', name='organizations:projects:list',
        responses={404: {}},
)
async def list_(session: SessionDep, organization: OrganizationDep) -> Sequence[Project]:
    await session.refresh(organization, ['projects'])
    return organization.projects


@api.post(
        '/', name='organizations:projects:create', status_code=201,
        responses={
            201: {
                'content': None,
                'headers': {
                    'Location': {
                        'description': 'URL of the created item',
                        'schema': {'type': 'string'},
                    },
                },
                'links': {
                    'detail': {
                        'operationId': 'organizations:projects:detail',
                        'parameters': {'project_id': '$response.header.Location#regex:/projects/(.+)/'},
                    },
                    'update': {
                        'operationId': 'organizations:projects:update',
                        'parameters': {'project_id': '$response.header.Location#regex:/projects/(.+)/'},
                    },
                    'delete': {
                        'operationId': 'organizations:projects:delete',
                        'parameters': {'project_id': '$response.header.Location#regex:/projects/(.+)/'},
                    },
                },
            },
            404: {},
            422: {},
        },
)
async def create(
        session: SessionDep, request: Request,
        organization: OrganizationDep, parameters: ProjectCreate,
) -> Response:
    entity = Project(organization=organization, **parameters.model_dump())
    session.add(entity)
    await session.commit()
    await session.refresh(entity)
    await session.refresh(organization)
    entity_url = request.app.url_path_for(
            'organizations:projects:detail',
            organization_id=organization.id, project_id=entity.id,
    )
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{project_id}')


async def _lookup(session: SessionDep, project_id: Int64) -> Project:
    result = await session.get(Project, project_id)
    if result is None:
        raise HTTPException(404, f'Project {project_id} not found')
    return result


ProjectDep = Annotated[Project, Depends(_lookup)]


@instance_api.get(
        '/', name='organizations:projects:detail',
        responses={404: {}},
)
async def detail(_organization: OrganizationDep, project: ProjectDep) -> Project:
    return project


@instance_api.put(
        '/', name='organizations:projects:update',
        status_code=204, responses={404: {}},
)
async def update(session: SessionDep, _organization: OrganizationDep, project: ProjectDep, parameters: ProjectUpdate):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert(hasattr(project, key))
        setattr(project, key, value)
    await session.commit()
    return Response(status_code=204)


@instance_api.delete(
        '/', name='organizations:projects:delete',
        status_code=204, responses={404: {}},
)
async def delete(session: SessionDep, _organization: OrganizationDep, project: ProjectDep):
    await session.delete(project)
    await session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
