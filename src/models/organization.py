from typing import TYPE_CHECKING

from pydantic import BaseModel, StrictBool
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Relationship

from .._util import Name
from ._util import Model
from .membership import Membership
from .resources import ResourceLimit

if TYPE_CHECKING:
    from .project import Project
    from .role import Role
    from .user import User


class Organization(AsyncAttrs, Model, table=True):
    name: Name
    locked: bool = False
    projects: list["Project"] = Relationship(back_populates="organization", cascade_delete=True)
    roles: list["Role"] = Relationship(back_populates="organization", cascade_delete=True)
    users: list["User"] = Relationship(back_populates="organizations", link_model=Membership)
    require_mfa: bool = False
    max_backups: int
    environments: str
    limits: list[ResourceLimit] = Relationship(back_populates="org", cascade_delete=True)


class OrganizationCreate(BaseModel):
    name: Name
    locked: StrictBool = False
    require_mfa: StrictBool = False
    max_backups: int
    environments: str


class OrganizationUpdate(BaseModel):
    name: Name | None = None
    locked: StrictBool | None = None
    require_mfa: StrictBool | None = None
    max_backups: int | None = None
    environments: str | None = None
