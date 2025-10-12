import json
import re
from importlib.resources import files
from typing import Any, Literal
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
from .organization import instance_api
from .organization.project.branch import branch_api
from .organization.project import projects_api
from .ressources import router as ressources_router
from .backupmonitor import *
from .ressources import monitor_resources
import logging
import signal
from threading import Thread, Event

from pydantic import BaseModel


from .db import engine
from sqlmodel import SQLModel

class _FastAPI(FastAPI):
    def openapi(self) -> dict[str, Any]:
        if self.openapi_schema:
            return self.openapi_schema

        def convert_path(path: str) -> str:
            pattern = r"^/admin/realms/\{realm\}"
            replacement = "/organizations/{organization_id}/projects/{project_id}/branches/{branch_id}/auth"
            return re.sub(pattern, replacement, path)

        def convert_method(method_spec: dict) -> dict:
            method_spec["tags"] = ["branch-auth"]
            return method_spec

        def convert_path_spec(path_spec: dict) -> dict:
            path_spec.setdefault("parameters", []).extend(
                [
                    {
                        "name": "organization_id",
                        "in": "path",
                        "required": True,
                        "schema": {
                            "type": "string",
                            "format": "ulid",
                            "pattern": "^[0-7][0-9A-HJKMNP-TV-Z]{25}$",
                            "minLength": 26,
                            "maxLength": 26,
                            "description": "A ULID (Universally Unique Lexicographically Sortable Identifier)",
                            "examples": [
                                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                                "01H945P9C3K2QJ8F7N6M4R2E8V",
                            ],
                            "title": "Organization Id",
                        },
                    },
                    {
                        "name": "branch_id",
                        "in": "path",
                        "required": True,
                        "schema": {
                            "type": "string",
                            "format": "ulid",
                            "pattern": "^[0-7][0-9A-HJKMNP-TV-Z]{25}$",
                            "minLength": 26,
                            "maxLength": 26,
                            "description": "A ULID (Universally Unique Lexicographically Sortable Identifier)",
                            "examples": [
                                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                                "01H945P9C3K2QJ8F7N6M4R2E8V",
                            ],
                            "title": "Branch Id",
                        },
                    },
                    {
                        "name": "project_id",
                        "in": "path",
                        "required": True,
                        "schema": {
                            "type": "string",
                            "format": "ulid",
                            "pattern": "^[0-7][0-9A-HJKMNP-TV-Z]{25}$",
                            "minLength": 26,
                            "maxLength": 26,
                            "description": "A ULID (Universally Unique Lexicographically Sortable Identifier)",
                            "examples": [
                                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                                "01H945P9C3K2QJ8F7N6M4R2E8V",
                            ],
                            "title": "Project Id",
                        },
                    },
                ]
            )
            path_spec["parameters"] = [param for param in path_spec["parameters"] if param["name"] != "realm"]
            for method, method_spec in ((k, v) for k, v in path_spec.items() if k != "parameters"):
                path_spec[method] = convert_method(method_spec)

            return path_spec

        openapi_schema = super().openapi()
        keycloak_openapi_schema = json.loads(files(__package__).joinpath("keycloak-26.4.0-api.json").read_text())

        openapi_schema["paths"].update(
            **{
                convert_path(path): convert_path_spec(spec)
                for path, spec in keycloak_openapi_schema["paths"].items()
                if path != "/admin/realms"
            }
        )
        openapi_schema["components"]["schemas"].update(**keycloak_openapi_schema["components"]["schemas"])
        return openapi_schema


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

_tags = [
    {"name": "user"},
    {"name": "organization"},
    {"name": "role", "parent": "organization"},
    {"name": "project", "parent": "organization"},
    {"name": "branch", "parent": "project"},
]


app = FastAPI(openapi_tags=_tags, root_path=settings.root_path)

router = APIRouter()

@router.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "vela"
    }

app.include_router(router)
app.include_router(organization_api)
app.include_router(user_api, prefix="/users")
app.include_router(roles_api, prefix="/roles")
app.include_router(backup_router)
app.include_router(ressources_router, prefix="/resources")
app.include_router(instance_api)
app.include_router(projects_api)
app.include_router(branch_api)

#_use_route_names_as_operation_ids(app)

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
async def on_startup():
    # create tables
    from . import models
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    # start async background monitor
    asyncio.create_task(run_monitor())
    asyncio.create_task(monitor_resources(60))

