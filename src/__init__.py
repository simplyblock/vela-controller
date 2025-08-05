from fastapi import FastAPI
from fastapi.routing import APIRoute

from . import models
from .api import api


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
app.include_router(api)
_use_route_names_as_operation_ids(app)


__all__ = ['app', 'models']
