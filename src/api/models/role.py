from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import BigInteger
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel

from .organization import Organization

if TYPE_CHECKING:
    from .user import User


class RoleUserLink(AsyncAttrs, SQLModel, table=True):
    role_id: int | None = Field(default=None, foreign_key="role.id", primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)


class Role(AsyncAttrs, SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_type=BigInteger)
    organization_id: int | None = Field(default=None, foreign_key="organization.id")
    organization: Organization | None = Relationship(back_populates="roles")
    users: list["User"] = Relationship(back_populates="roles", link_model=RoleUserLink)

    def dbid(self) -> int:
        if self.id is None:
            raise ValueError("Model not tracked in database")
        return self.id
