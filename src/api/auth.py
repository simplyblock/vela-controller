from typing import Annotated

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import ValidationError
from sqlmodel import select

from .db import SessionDep
from .models.user import JWT, User
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
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        raw_token = jwt.decode(credentials.credentials, settings.jwt_secret, algorithms=["HS256"])
        token = JWT.model_validate(raw_token)
    except (jwt.exceptions.PyJWTError, ValidationError) as e:
        raise HTTPException(401, str(e)) from e

    query = select(User).where(User.id == token.sub)
    db_user = (await session.exec(query)).unique().one_or_none()
    user = db_user if db_user is not None else User(id=token.sub)
    user.token = token
    return user


UserDep = Annotated[User, Depends(authenticated_user)]
