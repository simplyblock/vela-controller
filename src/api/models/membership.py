from uuid import UUID

from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, SQLModel
from ulid import ULID

from ..db import DBULID


class Membership(AsyncAttrs, SQLModel, table=True):
    organization_id: ULID = Field(foreign_key="organization.id", primary_key=True, sa_type=DBULID)
    user_id: UUID = Field(foreign_key="user.id", primary_key=True)

    __table_args__ = (UniqueConstraint("organization_id", "user_id", name="unique_membership"),)
