from cloudflare.types.user import Organization
from sqlmodel import select

from ..db import SessionDep
from ..models.role import AccessRight, Role, RoleAccessRight, RoleType


async def get_access_rights(session: SessionDep) -> list[AccessRight]:
    result = await session.execute(select(AccessRight))
    return result.scalars().all()


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
