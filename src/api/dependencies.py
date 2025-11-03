from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from .._util import Identifier
from ..models.branch import Branch
from ..models.organization import Organization
from ..models.project import Project
from ..models.role import Role
from ..models.user import User
from .auth import authenticated_user
from .db import engine


async def _get_session():
    async with AsyncSession(engine) as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(_get_session)]


async def _organization_lookup(session: SessionDep, organization_id: Identifier) -> Organization:
    try:
        return (await session.execute(select(Organization).where(Organization.id == organization_id))).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Organization {organization_id} not found") from e


OrganizationDep = Annotated[Organization, Depends(_organization_lookup)]


async def _project_lookup(session: SessionDep, organization: OrganizationDep, project_id: Identifier) -> Project:
    try:
        query = select(Project).where(Project.organization_id == organization.id, Project.id == project_id)
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_id} not found") from e


ProjectDep = Annotated[Project, Depends(_project_lookup)]


async def _role_lookup(session: SessionDep, organization: OrganizationDep, role_id: Identifier) -> Role:
    try:
        query = select(Role).where(Role.id == role_id, Role.organization_id == organization.id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Role {role_id} not found") from e


RoleDep = Annotated[Role, Depends(_role_lookup)]


async def branch_lookup(session: SessionDep, project: ProjectDep, branch_id: Identifier) -> Branch:
    try:
        query = select(Branch).where(Branch.project_id == project.id, Branch.id == branch_id)
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Branch {branch_id} not found") from e


BranchDep = Annotated[Branch, Depends(branch_lookup)]


AuthUserDep = Annotated[User, Depends(authenticated_user)]


async def user_lookup(session: SessionDep, user_id: UUID) -> User:
    query = select(User).where(User.id == user_id)
    user = (await session.execute(query)).scalars().one_or_none()
    if user is None:
        raise HTTPException(404, f"User {user_id} not found")
    return user


UserDep = Annotated[User, Depends(user_lookup)]


async def _member_lookup(organization: OrganizationDep, user: UserDep) -> User:
    if user not in await organization.awaitable_attrs.users:
        raise HTTPException(404, "User is not a member of this organization")
    return user


MemberDep = Annotated[User, Depends(_member_lookup)]
