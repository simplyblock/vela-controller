from typing import Annotated
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlmodel import select

from .db import SessionDep
from .models.user import User
from .settings import settings

# HTTPBearer returns 403 instead of 401. Avoid this by raising the error manually
security = HTTPBearer(auto_error=False)


async def authenticated_user(
        session: SessionDep,
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> User:
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail='Missing bearer token',
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        id_ = UUID(jwt.decode(credentials.credentials, settings.jwt_secret, algorithms=['HS256'])['sub'])
        query = select(User).where(User.id == id_)
        db_user = (await session.exec(query)).one_or_none()
        return db_user if db_user is not None else User(id=id_)
    except (
            jwt.exceptions.PyJWTError,
            ValueError,  # Invalid 'sub'
    ) as e:
        raise HTTPException(401, str(e)) from e


UserDep = Annotated[User, Depends(authenticated_user)]
