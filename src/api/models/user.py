from uuid import UUID

from sqlmodel import Field, Relationship, SQLModel

from .organization import Organization, OrganizationUserLink


class User(SQLModel, table=True):
    id: UUID = Field(primary_key=True)
    organizations: list[Organization] = Relationship(
            back_populates='users',
            link_model=OrganizationUserLink,
            sa_relationship_kwargs={'lazy': 'selectin'},
    )
