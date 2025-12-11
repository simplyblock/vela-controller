import asyncio
import json
import logging
import logging.config
import re
import sys
from importlib.resources import files
from typing import Any, Literal

from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from httpx import TimeoutException
from pydantic import BaseModel

from ..deployment.logflare import create_global_logflare_objects
from ..deployment.monitors.resize import ResizeMonitor
from ..exceptions import VelaLogflareError
from ._util.resourcelimit import create_system_resource_limits
from ._util.role import create_access_rights_if_emtpy
from .backup import router as backup_router
from .backupmonitor import run_backup_monitor
from .db import engine
from .organization import api as organization_api
from .resources import monitor_resources
from .resources import router as resources_router
from .settings import get_settings
from .system import api as system_api
from .user import api as user_api


def _logging_config() -> dict[str, Any]:
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    date_format = "%Y-%m-%dT%H:%M:%S%z"
    log_level = get_settings().log_level
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": log_format,
                "datefmt": date_format,
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
            },
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["default"],
                "level": log_level,
            },
            "uvicorn": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False,
            },
            "fastapi": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False,
            },
        },
    }


logging.config.dictConfig(_logging_config())
logger = logging.getLogger(__name__)


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
            method_spec["security"] = [{"HTTPBearer": []}]
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


async def _populate_db():
    # Python does not want us to get a string representation of this...
    migrations_path = str(files("simplyblock.vela.models.migrations")._paths[0])  # type: ignore[attr-defined]

    config = Config()
    config.set_main_option("script_location", migrations_path)
    command.upgrade(config, "head")

    async with engine.begin() as conn:
        await create_access_rights_if_emtpy(conn)
        await create_system_resource_limits(conn)


def _use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


_tags = [
    {"name": "user"},
    {"name": "organization"},
    {"name": "role", "parent": "organization"},
    {"name": "project", "parent": "organization"},
    {"name": "branch", "parent": "project"},
]

app = _FastAPI(openapi_tags=_tags, root_path=get_settings().root_path)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


class Status(BaseModel):
    service: Literal["vela"] = "vela"


@app.get("/health", response_model=Status)
def health():
    return Status()


app.include_router(organization_api, prefix="/organizations")
app.include_router(user_api, prefix="/users")
app.include_router(resources_router, prefix="/resources")
app.include_router(system_api, prefix="/system")
app.include_router(backup_router)
_use_route_names_as_operation_ids(app)


_resize_monitor = ResizeMonitor()


@app.on_event("startup")
async def on_startup():
    await _populate_db()
    try:
        await create_global_logflare_objects()
    except VelaLogflareError as exc:
        if not isinstance(exc.__cause__, TimeoutException):
            raise
        logger.error("Timeout while creating global logflare entities")
    # start async background monitor
    asyncio.create_task(run_backup_monitor())
    asyncio.create_task(monitor_resources(60))
    _resize_monitor.start()


@app.on_event("shutdown")
async def on_shutdown():
    await _resize_monitor.stop()


__all__ = ["app"]
