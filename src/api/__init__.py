from fastapi import Depends, FastAPI
from fastapi.routing import APIRoute
from sqlmodel import SQLModel

from .auth import authenticated_user
from .db import engine
from .organization import api as organization_api


async def _create_db_and_tables():
    from . import models  # Ensure models are registered # noqa
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


def _use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


app = FastAPI(dependencies=[Depends(authenticated_user)])


@app.on_event("startup")
async def on_startup():
    await _create_db_and_tables()


app.include_router(organization_api, prefix='/organizations')
_use_route_names_as_operation_ids(app)


__all__ = ['app']
