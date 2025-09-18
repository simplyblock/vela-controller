from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PrivateAttr
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.types import Text, TypeDecorator
from sqlmodel import Field as SQLField
from sqlmodel import Relationship, SQLModel

from .organization import Organization, OrganizationUserLink


class JWT(BaseModel):
    model_config = ConfigDict(extra="ignore")
    sub: UUID
    aal: Annotated[
        int,
        Field(ge=1, le=3),
        BeforeValidator(lambda s: int(s.removeprefix("aal"))),
    ] = 1

    def mfa(self) -> bool:
        return self.aal >= 2


class _JWTType(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, _dialect):
        return value.model_dump_json()

    def process_result_value(self, value, _dialect):
        return JWT.model_validate_json(value)


class User(AsyncAttrs, SQLModel, table=True):
    id: UUID = SQLField(primary_key=True)
    organizations: list[Organization] = Relationship(back_populates="users", link_model=OrganizationUserLink)
    _token: JWT | None = PrivateAttr(default=None)

    @property
    def token(self) -> JWT:
        if self._token is None:
            raise ValueError("User has no token")
        return self._token

    @token.setter
    def token(self, token: JWT):
        self._token = token


class UserPublic(BaseModel):
    id: UUID


class UserRequest(BaseModel):
    id: UUID
