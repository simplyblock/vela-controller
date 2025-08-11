from typing import Annotated
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ..settings import settings

# HTTPBearer returns 403 instead of 401. Avoid this by raising the error manually
security = HTTPBearer(auto_error=False)


def authenticated_user(credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)]):
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail='Missing bearer token',
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        return UUID(jwt.decode(credentials.credentials, settings.jwt_secret, ['HS256'])['sub'])
    except jwt.exceptions.PyJWTError as e:
        raise HTTPException(401, str(e)) from e
