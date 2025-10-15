import json
import logging
import re
from importlib.resources import files
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from httpx import TimeoutException
from pydantic import BaseModel
from sqlmodel import SQLModel

from ..deployment.logflare import create_global_logflare_objects
from ..exceptions import VelaLogflareError
from .db import engine
from .organization import api as organization_api
from .settings import settings
from .user import api as user_api


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


_tags = [
    {"name": "user"},
    {"name": "organization"},
    {"name": "role", "parent": "organization"},
    {"name": "project", "parent": "organization"},
    {"name": "branch", "parent": "project"},
]


app = _FastAPI(openapi_tags=_tags, root_path=settings.root_path)

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
    try:
        await create_global_logflare_objects()
    except VelaLogflareError as exc:
        if not isinstance(exc.__cause__, TimeoutException):
            raise
        logging.error("Timeout while creating global logflare entities")


class Status(BaseModel):
    service: Literal["vela"] = "vela"


@app.get("/health", response_model=Status)
def health():
    return Status()


app.include_router(organization_api, prefix="/organizations")
app.include_router(user_api, prefix="/users")
_use_route_names_as_operation_ids(app)


__all__ = ["app"]
