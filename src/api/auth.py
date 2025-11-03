import re
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWK, PyJWKClient, decode
from jwt.exceptions import PyJWTError
from pydantic import ValidationError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from ..models.user import JWT, User
from .settings import settings

# HTTPBearer returns 403 instead of 401. Avoid this by raising the error manually
security = HTTPBearer(auto_error=False)

# This is simplistic but will do for now
_HTTP_URL_PATTERN = re.compile(r"^https?://")


def _decode(token: str):
    key: PyJWK | str
    if re.match(_HTTP_URL_PATTERN, settings.jwt_secret):
        jwks_client = PyJWKClient(settings.jwt_secret)
        key = jwks_client.get_signing_key_from_jwt(token)
    else:
        key = settings.jwt_secret

    return decode(token, key, algorithms=settings.jwt_algorithms, options={"verify_aud": False})


async def user_by_id(session: AsyncSession, id_: UUID):
    query = select(User).where(User.id == id_)
    db_user = (await session.execute(query)).unique().scalars().one_or_none()
    return db_user if db_user is not None else User(id=id_)


async def authenticated_user(
    session: AsyncSession,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> User:
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        raw_token = _decode(credentials.credentials)
        token = JWT.model_validate(raw_token)
    except (PyJWTError, ValidationError) as e:
        raise HTTPException(401, str(e)) from e

    user = await user_by_id(session, id_=token.sub)
    user.token = token
    return user
