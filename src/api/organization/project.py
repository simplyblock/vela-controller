from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from ...models.project import Project, ProjectCreate
from ..organization import OrganizationDep
from ..util import DB

api = APIRouter(prefix='/projects')


@api.get('/', name='organizations:projects:list')
def list_(db: DB, organization_id: UUID) -> list[Project]:
    return [
            project
            for project
            in Project.list(db)
            if project.organization == organization_id
    ]


@api.post(
        '/', name='organizations:projects:create', status_code=201,
        responses={201: {
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
                    'parameters': {'project_id': '$response.header.Location#regex:/projects/(.+)'},
                },
                'delete': {
                    'operationId': 'organizations:projects:delete',
                    'parameters': {'project_id': '$response.header.Location#regex:/projects/(.+)'},
                },
            },
        }},
)
def create(db: DB, request: Request, organization: OrganizationDep, parameters: ProjectCreate) -> Response:
    entity = Project.create(db, organization=organization.id, **parameters.model_dump())
    entity_url = request.app.url_path_for('organizations:projects:detail', organization_id=entity.id)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{project_id}')


def _lookup_project(db: DB, project_id: UUID) -> Project:
    try:
        return Project.get(db, project_id)
    except KeyError as e:
        raise HTTPException(404, str(e)) from e


ProjectDep = Annotated[Project, Depends(_lookup_project)]


@api.get('/', name='organizations:projects:detail')
def detail(_organization: OrganizationDep, project: ProjectDep) -> Project:
    return project


@api.delete('/', name='organizations:projects:delete', status_code=204)
def delete(db: DB, _organization: OrganizationDep, project: ProjectDep):
    project.delete(db)
    return Response(204)


api.include_router(instance_api)
