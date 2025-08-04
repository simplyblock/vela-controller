from pydantic import BaseModel

from ._base import Model


class Organization(Model):
    name: str


class OrganizationCreate(BaseModel):
    name: str
