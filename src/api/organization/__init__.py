from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from ...models.organization import Organization, OrganizationCreate
from ...models.project import Project
from ..util import DB

api = APIRouter(prefix='/organizations')


@api.get('/', name='organizations:list')
def list_(db: DB) -> list[Organization]:
    return Organization.list(db)


@api.post(
        '/', name='organizations:create', status_code=201,
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
                    'operationId': 'organizations:detail',
                    'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)'},
                },
                'delete': {
                    'operationId': 'organizations:delete',
                    'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)'},
                },
            },
        }},
)
def create(db: DB, request: Request, parameters: OrganizationCreate) -> Response:
    entity = Organization.create(db, **parameters.model_dump())
    entity_url = request.app.url_path_for('organizations:detail', organization_id=entity.id)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{organization_id}')


def _lookup_organization(db: DB, organization_id: UUID) -> Organization:
    try:
        return Organization.get(db, organization_id)
    except KeyError as e:
        raise HTTPException(404, str(e)) from e


OrganizationDep = Annotated[Organization, Depends(_lookup_organization)]


@instance_api.get(
        '/', name='organizations:detail',
        responses={
            404: {},
            422: {},
        },
)
def detail(organization: OrganizationDep) -> Organization:
    return organization


@instance_api.delete(
        '/', name='organizations:delete',
        status_code=204,
        responses={
            404: {},
            422: {},
        },
)
def delete_organization(db: DB, organization: OrganizationDep):
    tr = db.create_transaction()
    for project in Project.by_organization(tr, organization.id):
        project.delete(tr)
    organization.delete(tr)
    tr.commit().wait()
    return Response(status_code=204)


api.include_router(instance_api)
