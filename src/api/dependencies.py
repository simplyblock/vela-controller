from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlmodel import select
from datetime import timedelta

from .settings import get_settings

from .._util import Identifier
from ..models.backups import BackupEntry
from ..models.branch import Branch, BranchRestore, BranchServiceStatus
from ..models.organization import Organization
from ..models.project import Project
from ..models.role import Role
from ..models.user import User
from .auth import authenticated_user
from .db import SessionDep


async def organization_lookup(session: SessionDep, organization_id: Identifier) -> Organization:
    try:
        return (await session.execute(select(Organization).where(Organization.id == organization_id))).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Organization {organization_id} not found") from e


OrganizationDep = Annotated[Organization, Depends(organization_lookup)]


async def project_lookup(session: SessionDep, project_id: Identifier) -> Project:
    try:
        query = select(Project).where(Project.id == project_id)
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        raise HTTPException(404, f"Project {project_id} not found") from e


ProjectDep = Annotated[Project, Depends(project_lookup)]


async def _role_lookup(session: SessionDep, role_id: Identifier) -> Role:
    try:
        query = select(Role).where(Role.id == role_id)
        return (await session.exec(query)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"Role {role_id} not found") from e


RoleDep = Annotated[Role, Depends(_role_lookup)]


async def branch_lookup(session: SessionDep, branch_id: Identifier) -> Branch:
    try:
        query = select(Branch).where(Branch.id == branch_id)
        branch = (await session.execute(query)).scalars().one()
        status_value = branch.status
        if (
            status_value
            and BranchServiceStatus._value2member_map_.get(str(status_value)) == BranchServiceStatus.DELETING
        ):
            raise HTTPException(status_code=409, detail="Branch is being deleted and cannot be manipulated.")
        return branch
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


async def _restore_backup_lookup(
    session: SessionDep,
    branch: BranchDep,
    parameters: BranchRestore,
) -> BackupEntry:
    if parameters.backup_id is not None:
        query = select(BackupEntry).where(
            BackupEntry.id == parameters.backup_id,
            BackupEntry.branch_id == branch.id,
        )
        backup = (await session.execute(query)).scalars().one_or_none()
        if backup is None:
            raise HTTPException(status_code=404, detail=f"Backup {parameters.backup_id} not found for this branch")
    elif parameters.recovery_target_time is not None:
        query = (
            select(BackupEntry)
            .where(BackupEntry.branch_id == branch.id)
            .where(BackupEntry.created_at <= parameters.recovery_target_time)
            .order_by(BackupEntry.created_at.desc())
            .limit(1)
        )
        backup = (await session.execute(query)).scalars().one_or_none()
        if backup is None:
            raise HTTPException(status_code=404, detail="No valid backup snapshot found before the requested recovery time")
        
        max_retention = timedelta(days=get_settings().pitr_wal_retention_days)
        if parameters.recovery_target_time - backup.created_at > max_retention:
            raise HTTPException(
                status_code=422,
                detail="Requested recovery time exceeds the WAL archive retention policy for the nearest snapshot."
            )
    else:
        raise HTTPException(status_code=400, detail="Either backup_id or recovery_target_time must be provided")
    
    return backup


RestoreBackupDep = Annotated[BackupEntry, Depends(_restore_backup_lookup)]
