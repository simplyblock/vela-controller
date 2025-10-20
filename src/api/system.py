from fastapi import APIRouter, Depends
from sqlmodel import select

from .auth import authenticated_user
from .db import SessionDep
from .models.role import AccessRight

api = APIRouter(dependencies=[Depends(authenticated_user)], tags=["system"])


@api.get("/available-permissions/")
async def list_available_permissions(
    session: SessionDep,
) -> list[str]:
    """
    List all access rights defined in the system.
    """
    stmt = select(AccessRight.entry)
    result = await session.execute(stmt)
    entries = [row[0] for row in result.all()]
    return entries
