from fastapi import APIRouter, Depends

from .auth import authenticated_user
from .organization import api as organization_api

router = APIRouter(dependencies=[Depends(authenticated_user)])
router.include_router(organization_api, prefix='/organizations')
