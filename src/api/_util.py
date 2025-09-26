from functools import wraps
from typing import Annotated, Any

from asyncpg import UniqueViolationError
from fastapi import HTTPException, Request
from pydantic import BaseModel, BeforeValidator, PlainSerializer, WithJsonSchema
from ulid import ULID


class HTTPError(BaseModel):
    detail: str | dict[str, Any]


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


def validate_ulid(v: Any) -> ULID:
    if isinstance(v, ULID):
        return v
    if isinstance(v, str):
        return ULID.from_str(v)
    raise ValueError("Invalid ULID format")


ULIDType = Annotated[
    ULID,
    BeforeValidator(validate_ulid),
    PlainSerializer(lambda ulid: str(ulid), return_type=str),
    WithJsonSchema(
        {
            "type": "string",
            "format": "ulid",
            "pattern": r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$",
            "minLength": 26,
            "maxLength": 26,
            "description": "A ULID (Universally Unique Lexicographically Sortable Identifier)",
            "examples": ["01ARZ3NDEKTSV4RRFFQ69G5FAV", "01H945P9C3K2QJ8F7N6M4R2E8V"],
            "title": "ULID",
        }
    ),
]
