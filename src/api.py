from uuid import UUID

from fastapi import APIRouter, HTTPException, Response, Request
import fdb

from .models.organization import Organization, OrganizationCreate


db = fdb.open()


api = APIRouter()


org_api = APIRouter(prefix='/organizations')


@org_api.get('/')
def list_organizations() -> list[Organization]:
    return Organization.list(db)


@org_api.post(
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
def create_organization(request: Request, parameters: OrganizationCreate) -> Response:
    entity = Organization.create(db, **parameters.model_dump())
    entity_url = request.app.url_path_for('organizations:detail', organization_id=entity.id)
    return Response(status_code=201, headers={'Location': entity_url})


instance_org_api = APIRouter(prefix='/{organization_id}')


@instance_org_api.get(
        '/', name='organizations:detail',
        responses={
            404: {},
            422: {},
        },
)
def get_organization(organization_id: UUID):
    try:
        return Organization.get(db, organization_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@instance_org_api.delete(
        '/', name='organizations:delete',
        status_code=204,
        responses={
            404: {},
            422: {},
        },
)
def delete_organization(organization_id: UUID):
    try:
        organization = Organization.get(db, organization_id)
        organization.delete(db)
        return Response(status_code=204)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


org_api.include_router(instance_org_api)


api.include_router(org_api)
