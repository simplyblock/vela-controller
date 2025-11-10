from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, SQLModel

from ._util import Model


class Membership(AsyncAttrs, SQLModel, table=True):
    organization_id: int | None = Model.foreign_key_field("organization", nullable=True, primary_key=True)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)
