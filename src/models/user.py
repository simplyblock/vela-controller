from typing import Annotated
from uuid import UUID

from fastapi import Depends
from fastapi_users import schemas
from fastapi_users.db import SQLAlchemyBaseUserTableUUID, SQLAlchemyUserDatabase
from sqlalchemy.orm import DeclarativeBase

from ..db import SessionDep


class UserRead(schemas.BaseUser[UUID]):
    pass


class UserCreate(schemas.BaseUserCreate):
    pass


class UserUpdate(schemas.BaseUserUpdate):
    pass


class Base(DeclarativeBase):
    pass


class User(SQLAlchemyBaseUserTableUUID, Base):
    pass


async def _get_user_db(session: SessionDep):
    yield SQLAlchemyUserDatabase(session, User)

UserDBDep = Annotated[SQLAlchemyUserDatabase, Depends(_get_user_db)]
