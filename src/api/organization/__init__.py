from collections.abc import Sequence

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from ...deployment import delete_deployment
from .._util import Forbidden, NotFound, Unauthenticated
from ..auth import UserDep, authenticated_user
from ..db import SessionDep
from ..models.organization import Organization, OrganizationCreate, OrganizationDep, OrganizationUpdate
from .project import api as project_api

api = APIRouter(dependencies=[Depends(authenticated_user)])


@api.get(
        '/', name='organizations:list',
        responses={401: Unauthenticated},
)
async def list_(user: UserDep) -> Sequence[Organization]:
    return await user.awaitable_attrs.organizations


@api.post(
        '/', name='organizations:create', status_code=201,
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
                        'operationId': 'organizations:detail',
                        'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)/'},
                    },
                    'update': {
                        'operationId': 'organizations:update',
                        'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)/'},
                    },
                    'delete': {
                        'operationId': 'organizations:delete',
                        'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)/'},
                    },
                    'create_project': {
                        'operationId': 'organizations:projects:create',
                        'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)/'},
                    },
                    'list_projects': {
                        'operationId': 'organizations:projects:list',
                        'parameters': {'organization_id': '$response.header.Location#regex:/organizations/(.+)/'},
                    },
                },
            },
            401: Unauthenticated,
        },
)
async def create(
        session: SessionDep,
        request: Request,
        parameters: OrganizationCreate,
        user: UserDep,
) -> Response:
    entity = Organization(**parameters.model_dump(), users=[user])
    session.add(entity)
    await session.commit()
    await session.refresh(entity)
    entity_url = request.app.url_path_for('organizations:detail', organization_id=entity.id)
    return Response(status_code=201, headers={'Location': entity_url})


async def _user_in_organization(user: UserDep, organization: OrganizationDep):
    if user not in await organization.awaitable_attrs.users:
        raise HTTPException(403, detail='Unauthorized access')


instance_api = APIRouter(
        prefix='/{organization_id}',
        dependencies=[Depends(_user_in_organization)],
)


@instance_api.get(
        '/', name='organizations:detail',
        responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(organization: OrganizationDep) -> Organization:
    return organization


@instance_api.put(
        '/', name='organizations:update', status_code=204,
        responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update(session: SessionDep, organization: OrganizationDep, parameters: OrganizationUpdate):
    for key, value in parameters.model_dump(exclude_unset=True, exclude_none=True).items():
        assert(hasattr(organization, key))
        setattr(organization, key, value)
    await session.commit()
    return Response(status_code=204)


@instance_api.delete(
        '/', name='organizations:delete', status_code=204,
        responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(session: SessionDep, organization: OrganizationDep):
    for project in await organization.awaitable_attrs.projects:
        delete_deployment(project.dbid())

    await session.delete(organization)
    await session.commit()
    return Response(status_code=204)


instance_api.include_router(project_api, prefix='/projects')


api.include_router(instance_api)
