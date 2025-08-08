from fastapi import APIRouter

from .organization import api as organization_api

router = APIRouter()
router.include_router(organization_api, prefix='/organizations')
