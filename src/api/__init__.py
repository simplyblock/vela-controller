from fastapi import APIRouter

from .organization import api as organization_api

api = APIRouter()
api.include_router(organization_api)
