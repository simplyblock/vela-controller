from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, SQLModel


class Membership(AsyncAttrs, SQLModel, table=True):
    organization_id: int | None = Field(default=None, foreign_key="organization.id", primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)
