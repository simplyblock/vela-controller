from uuid import UUID

import fdb
from fastapi import APIRouter, Response, Request

from .models.organization import Organization, OrganizationCreate


db = fdb.open()


api = APIRouter()


org_api = APIRouter(prefix='/organizations')


@org_api.get('/')
def list_organizations() -> list[Organization]:
    return Organization.list(db)


@org_api.post('/', name='organizations:create', status_code=201, responses={201: {"content": None}})
def create_organization(request: Request, parameters: OrganizationCreate) -> Response:
    entity = Organization.create(db, **parameters.model_dump())
    entity_url = request.app.url_path_for('organizations:detail', organization_id=entity.id)
    return Response(status_code=201, headers={'Location': entity_url})



instance_org_api = APIRouter(prefix='/{organization_id}')


@instance_org_api.get('/', name='organizations:detail')
def get_organization(organization_id: UUID):
    return Organization.get(db, organization_id)


org_api.include_router(instance_org_api)


api.include_router(org_api)
