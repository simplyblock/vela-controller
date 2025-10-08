from typing import Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute, APIRouter
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import SQLModel
from .db import engine
from .organization import api as organization_api
from .settings import settings
from .user import api as user_api
from .roles_access_rights import router as roles_api
from .backup import router as backup_router
from .ressources import router as ressources_router
from .backupmonitor import *
import logging
import signal
from threading import Thread, Event

from pydantic import BaseModel


async def _create_db_and_tables():
    from . import models  # Ensure models are registered # noqa

    async with engine.begin() as conn:
        await conn.execute(text("SET search_path TO public"))
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
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

router = APIRouter()

@router.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "vela"
    }

app.include_router(router)
app.include_router(organization_api, prefix="/organizations")
app.include_router(user_api, prefix="/users")
app.include_router(roles_api, prefix="/roles")
app.include_router(backup_router)
app.include_router(ressources_router)

_use_route_names_as_operation_ids(app)

@app.on_event("startup")
async def on_startup():
    await _create_db_and_tables()

class Status(BaseModel):
    service: Literal["vela"] = "vela"

__all__ = ["app"]

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import asyncio

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(run_monitor())
