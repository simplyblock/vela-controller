from typing import Literal

from fastapi import FastAPI
from fastapi.routing import APIRoute
from pydantic import BaseModel
from sqlmodel import SQLModel
from fastapi.middleware.cors import CORSMiddleware

from .db import engine
from .organization import api as organization_api
from .settings import settings


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


app = FastAPI(root_path=settings.root_path)

# Get origins from settings, fallback to defaults if empty
origins = [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.on_event("startup")
async def on_startup():
    await _create_db_and_tables()


class Status(BaseModel):
    service: Literal["vela"] = "vela"


@app.get("/health", response_model=Status)
def health():
    return Status()


app.include_router(organization_api, prefix="/organizations")
_use_route_names_as_operation_ids(app)


__all__ = ["app"]
