from fastapi import APIRouter

from .organization import api as organization_api
from .organization.project import api as project_api

organization_api.include_router(project_api, prefix='/{organization_id}')

api = APIRouter()
api.include_router(organization_api)
