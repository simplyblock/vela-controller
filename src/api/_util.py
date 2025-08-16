from functools import wraps
from typing import Annotated, Any

from asyncpg import UniqueViolationError
from fastapi import HTTPException
from pydantic import BaseModel, StringConstraints

Slug = Annotated[str, StringConstraints(
        pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$',
        max_length=50,
)]


class HTTPError(BaseModel):
    detail: str | dict[str, Any]


NotFound = {'model': HTTPError, 'description': 'Not found'}
Forbidden = {'model': HTTPError, 'description': 'Forbidden'}
Unauthenticated = {'model': HTTPError, 'description': 'Not authenticated'}
Conflict = {'model': HTTPError, 'description': 'Conflict'}


def handle_unique_violation(f):
    @wraps
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except UniqueViolationError as e:
            raise HTTPException(409, 'Non-unique entity') from e

    return wrapper
