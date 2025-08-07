from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.exc import NoResultFound
from sqlmodel import select

from ...db import SessionDep
from ...models.project import Project, ProjectCreate, ProjectUpdate
from ..organization import OrganizationDep

api = APIRouter(prefix='/projects')


@api.get(
        '/', name='organizations:projects:list',
        responses={404: {}},
)
def list_(organization: OrganizationDep) -> Sequence[Project]:
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
def create(session: SessionDep, request: Request, organization: OrganizationDep, parameters: ProjectCreate) -> Response:
    entity = Project(organization=organization, **parameters.model_dump())
    session.add(entity)
    session.commit()
    entity_url = request.app.url_path_for(
            'organizations:projects:detail',
            organization_id=organization.id, project_id=entity.id,
    )
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{project_id}')


def _lookup(session: SessionDep, project_id: int) -> Project:
    try:
        return session.exec(select(Project).where(Project.id == project_id)).one()
    except NoResultFound as e:
        raise HTTPException(404, str(e)) from e


ProjectDep = Annotated[Project, Depends(_lookup)]


@instance_api.get(
        '/', name='organizations:projects:detail',
        responses={404: {}},
)
def detail(_organization: OrganizationDep, project: ProjectDep) -> Project:
    return project


@instance_api.put(
        '/', name='organizations:projects:update',
        status_code=204, responses={404: {}},
)
def update(session: SessionDep, _organization: OrganizationDep, project: ProjectDep, parameters: ProjectUpdate):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert(hasattr(project, key))
        setattr(project, key, value)
    session.commit()
    return Response(status_code=204)


@instance_api.delete(
        '/', name='organizations:projects:delete',
        status_code=204, responses={404: {}},
)
def delete(session: SessionDep, _organization: OrganizationDep, project: ProjectDep):
    session.delete(project)
    session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
