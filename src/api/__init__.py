from typing import Any, Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from pydantic import BaseModel
from sqlmodel import SQLModel
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from .db import engine
from .organization import api as organization_api
from .settings import settings


def _configure_sentry() -> None:
    if settings.sentry_dsn is None:
        return

    sentry_options: dict[str, Any] = {
        "dsn": str(settings.sentry_dsn),
        "integrations": [FastApiIntegration(), SqlalchemyIntegration()],
        "send_default_pii": False,
    }

    if settings.sentry_traces_sample_rate is not None:
        sentry_options["traces_sample_rate"] = settings.sentry_traces_sample_rate

    if settings.sentry_profiles_sample_rate is not None:
        sentry_options["profiles_sample_rate"] = settings.sentry_profiles_sample_rate

    if settings.sentry_environment:
        sentry_options["environment"] = settings.sentry_environment

    sentry_sdk.init(**sentry_options)


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


_configure_sentry()


app = FastAPI(root_path=settings.root_path)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
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
