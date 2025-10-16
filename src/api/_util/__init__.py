from functools import wraps
from typing import Annotated, Any

from asyncpg import UniqueViolationError
from fastapi import HTTPException, Request
from pydantic import BaseModel, Field


class HTTPError(BaseModel):
    detail: str | dict[str, Any]


Port = Annotated[int, Field(gt=0, lt=2**16)]

NotFound = {"model": HTTPError, "description": "Not found"}
Forbidden = {"model": HTTPError, "description": "Forbidden"}
Unauthenticated = {"model": HTTPError, "description": "Not authenticated"}
Conflict = {"model": HTTPError, "description": "Conflict"}


def handle_unique_violation(f):
    @wraps
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except UniqueViolationError as e:
            raise HTTPException(409, "Non-unique entity") from e

    return wrapper


def url_path_for(request: Request, name: str, **kwargs) -> str:
    return request.scope.get("root_path") + request.app.url_path_for(name, **kwargs)
