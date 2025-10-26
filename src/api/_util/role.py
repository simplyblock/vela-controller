from typing import get_args

from sqlalchemy.ext.asyncio import AsyncConnection
from sqlmodel import insert, select
from ulid import ULID

from ..._util import Identifier
from ..db import SessionDep
from ..models.branch import Branch
from ..models.role import AccessRight, AccessRightPublic, Organization, Role, RoleAccessRight, RoleType, RoleUserLink


async def clone_user_role_assignment(
    session: SessionDep,
    source_branch_id: Identifier,
    target: Branch,
):
    result = await session.execute(select(RoleUserLink).where(RoleUserLink.branch_id == source_branch_id))
    assignments = result.scalars().all()

    with session.no_autoflush:
        for assignment in assignments:
            await session.merge(
                RoleUserLink(
                    organization_id=assignment.organization_id,
                    project_id=assignment.project_id,
                    branch_id=target.id,
                    role_id=assignment.role_id,
                    user_id=assignment.user_id,
                    env_type=assignment.env_type,
                )
            )
    await session.commit()
    await session.refresh(target)


def get_role_type(access_right: AccessRightPublic) -> RoleType:
    name = str(access_right)
    if name.startswith("org:"):
        return RoleType.organization
    elif name.startswith("env:"):
        return RoleType.environment
    elif name.startswith("project:"):
        return RoleType.project
    elif name.startswith("branch:"):
        return RoleType.branch
    else:
        raise ValueError(f"Invalid access right: {name}")


async def create_access_rights_if_emtpy(conn: AsyncConnection):
    result = await conn.execute(select(AccessRight))
    if len(list(result.scalars().all())) == 0:
        for access_right_public in get_args(AccessRightPublic):
            await conn.execute(
                insert(AccessRight).values(
                    id=ULID(),
                    entry=access_right_public,
                    role_type=get_role_type(access_right_public).name,
                )
            )


async def get_access_rights(session: SessionDep) -> list[AccessRight]:
    result = await session.execute(select(AccessRight))
    return list(result.scalars().all())


async def create_organization_admin_role(session: SessionDep, organization: Organization) -> Role:
    role = Role(
        role_type=RoleType.organization,
        is_active=True,
        is_deletable=False,
        name="Organization Admin",
        organization_id=organization.id,
    )
    session.add(role)
    await session.commit()
    await session.refresh(role)
    await session.refresh(organization)

    result = await session.execute(select(AccessRight).where(AccessRight.entry == "org:owner:admin"))
    access_right = result.scalars().one()
    await session.refresh(access_right)
    role_access_right = RoleAccessRight(
        organization_id=organization.id, role_id=role.id, access_right_id=access_right.id
    )
    session.add(role_access_right)

    await session.commit()
    await session.refresh(role)
    await session.refresh(organization)
    return role
