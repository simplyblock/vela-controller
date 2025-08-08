from fastapi import APIRouter, Depends

from .auth import authenticated_user
from .organization import api as organization_api
from .vela import api as vela_api

router = APIRouter(dependencies=[Depends(authenticated_user)])
router.include_router(organization_api, prefix='/organizations')
router.include_router(vela_api, prefix="/api")
