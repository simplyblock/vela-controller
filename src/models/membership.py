from uuid import UUID

from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, SQLModel

from ._util import Model


class Membership(AsyncAttrs, SQLModel, table=True):
    organization_id: int | None = Model.foreign_key_field("organization", nullable=True, primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)

    __table_args__ = (UniqueConstraint("organization_id", "user_id", name="unique_membership"),)
