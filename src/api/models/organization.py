from typing import TYPE_CHECKING

from fastapi import HTTPException
from pydantic import BaseModel, StrictBool
from .membership import Membership

if TYPE_CHECKING:
    from .project import Project
    from .role import Role
    from .user import User

from .backups import *

class Organization(AsyncAttrs, Model, table=True):
    name: Name = Field(unique=True)
    locked: bool = False
    projects: list["Project"] = Relationship(back_populates="organization", cascade_delete=True)
    roles: list["Role"] = Relationship(back_populates="organization", cascade_delete=True)
    users: list["User"] = Relationship(back_populates="organizations", link_model=Membership)
    schedules: list["BackupSchedule"] = Relationship(back_populates="organization")

    max_backups: int

    require_mfa: bool = False

class OrganizationCreate(BaseModel):
    name: Name
    locked: StrictBool = False
    require_mfa: StrictBool = False


class OrganizationUpdate(BaseModel):
    name: Name | None = None
    locked: StrictBool | None = None
    require_mfa: StrictBool | None = None


async def _lookup(session: SessionDep, organization_id: Identifier) -> Organization:
    try:
        return (await session.exec(select(Organization).where(Organization.id == organization_id))).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Organization {organization_id} not found") from e


OrganizationDep = Annotated[Organization, Depends(_lookup)]
