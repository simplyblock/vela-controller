from fastapi import FastAPI
from fastapi.routing import APIRoute
from sqlmodel import SQLModel

from . import models
from .api import api
from .db import engine


def _create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def _use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


app = FastAPI()


@app.on_event("startup")
def on_startup():
    _create_db_and_tables()


app.include_router(api)
_use_route_names_as_operation_ids(app)


__all__ = ['app', 'models']
