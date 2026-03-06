from datetime import datetime, timedelta
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlmodel import select

from .._util import Identifier
from ..models.backups import BackupEntry
from ..models.branch import Branch, BranchApiKey, BranchRestore, BranchServiceStatus
from ..models.organization import Organization
from ..models.project import Project
from ..models.role import Role
from ..models.user import User
from .auth import authenticated_user
from .db import SessionDep
from .settings import get_settings


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


async def backup_lookup(session: SessionDep, backup_id: Identifier, branch_id: Identifier | None = None) -> BackupEntry:
    query = select(BackupEntry).where(BackupEntry.id == backup_id)
    if branch_id is not None:
        query = query.where(BackupEntry.branch_id == branch_id)
    try:
        return (await session.execute(query)).scalars().one()
    except NoResultFound as e:
        detail = f"Backup {backup_id} not found"
        if branch_id is not None:
            detail += f" for branch {branch_id}"
        raise HTTPException(404, detail) from e


async def resolve_pitr_backup(
    session: SessionDep,
    branch_id: Identifier,
    recovery_target_time: datetime,
) -> BackupEntry:
    query = (
        select(BackupEntry)
        .where(BackupEntry.branch_id == branch_id)
        .where(BackupEntry.created_at <= recovery_target_time)
        .order_by(cast("Any", BackupEntry.created_at).desc())
        .limit(1)
    )
    backup = (await session.execute(query)).scalars().one_or_none()
    if backup is None:
        raise HTTPException(
            status_code=404,
            detail="No valid backup snapshot found before the requested recovery time",
        )

    max_retention = timedelta(days=get_settings().pitr_wal_retention_days)
    if recovery_target_time - backup.created_at > max_retention:
        raise HTTPException(
            status_code=422,
            detail="Requested recovery time exceeds the WAL archive retention policy for the nearest snapshot.",
        )

    return backup


async def _restore_backup_lookup(
    session: SessionDep,
    branch: BranchDep,
    parameters: BranchRestore,
) -> BackupEntry:
    if parameters.backup_id is not None:
        return await backup_lookup(session, parameters.backup_id, branch_id=branch.id)

    if parameters.recovery_target_time is not None:
        return await resolve_pitr_backup(session, branch.id, parameters.recovery_target_time)

    raise HTTPException(status_code=400, detail="Either backup_id or recovery_target_time must be provided")


RestoreBackupDep = Annotated[BackupEntry, Depends(_restore_backup_lookup)]


async def _api_key_lookup(session: SessionDep, api_key_id: Identifier) -> BranchApiKey:
    statement = select(BranchApiKey).where(BranchApiKey.id == api_key_id)
    try:
        return (await session.exec(statement)).one()
    except NoResultFound as e:
        raise HTTPException(404, f"API Key {api_key_id} not found") from e


ApiKeyDep = Annotated[BranchApiKey, Depends(_api_key_lookup)]
