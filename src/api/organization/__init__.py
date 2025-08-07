from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.exc import NoResultFound
from sqlmodel import select

from ...db import SessionDep
from ...models.organization import Organization, OrganizationCreate, OrganizationUpdate

api = APIRouter(prefix='/organizations')


@api.get('/', name='organizations:list')
def list_(session: SessionDep) -> Sequence[Organization]:
    return session.exec(select(Organization)).all()


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
                'update': {
                    'operationId': 'organizations:update',
                    'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)'},
                },
                'delete': {
                    'operationId': 'organizations:delete',
                    'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)'},
                },
                'create_project': {
                    'operationId': 'organizations:projects:create',
                    'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)'},
                },
                'list_projects': {
                    'operationId': 'organizations:projects:list',
                    'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)'},
                },
            },
        }},
)
def create(session: SessionDep, request: Request, parameters: OrganizationCreate) -> Response:
    entity = Organization(**parameters.model_dump())
    session.add(entity)
    session.commit()
    entity_url = request.app.url_path_for('organizations:detail', organization_id=entity.id)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{organization_id}')


def _lookup(session: SessionDep, organization_id: int) -> Organization:
    try:
        return session.exec(select(Organization).where(Organization.id == organization_id)).one()
    except NoResultFound as e:
        raise HTTPException(404, str(e)) from e


OrganizationDep = Annotated[Organization, Depends(_lookup)]


@instance_api.get(
        '/', name='organizations:detail',
        responses={
            404: {},
            422: {},
        },
)
def detail(organization: OrganizationDep) -> Organization:
    return organization


@instance_api.put(
        '/', name='organizations:update', status_code=204,
        responses={404: {}},
)
def update(session: SessionDep, organization: OrganizationDep, parameters: OrganizationUpdate):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert(hasattr(organization, key))
        setattr(organization, key, value)
    session.commit()
    return Response(status_code=204)


@instance_api.delete(
        '/', name='organizations:delete', status_code=204,
        responses={404: {}},
)
def delete(session: SessionDep, organization: OrganizationDep):
    session.delete(organization)
    session.commit()
    return Response(status_code=204)


api.include_router(instance_api)
