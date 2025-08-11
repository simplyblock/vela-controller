from typing import Any

from pydantic import BaseModel


class HTTPError(BaseModel):
    detail: str | dict[str, Any]


NotFound = {'model': HTTPError, 'description': 'Not found'}
Unauthenticated = {'model': HTTPError, 'description': 'Not authenticated'}
