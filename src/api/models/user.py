from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship, SQLModel

from .organization import Organization, OrganizationUserLink


class User(AsyncAttrs, SQLModel, table=True):
    id: UUID = Field(primary_key=True)
    organizations: list[Organization] = Relationship(back_populates='users', link_model=OrganizationUserLink)
