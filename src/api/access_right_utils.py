from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from ..models.role import AccessRight, PermissionCheckContextPublic, Role, RoleAccessRight, RoleUserLink


# Wildcard matcher
def match_access(required: str, rights: list[str]) -> bool:
    """
    Returns True if the user rights include the required access.
    Supports wildcards '*' in sub-entity or action, but main entity must match exactly.
    """
    req_entity, req_sub, req_action = required.split(":")

    for right in rights:
        entity, sub, action = right.split(":")
        if entity != req_entity:
            continue
        if sub != "*" and sub != req_sub:
            continue
        if action != "*" and action != req_action:
            continue
        return True
    return False


async def get_user_rights(session: AsyncSession, user_id: UUID, context: PermissionCheckContextPublic) -> list[str]:
    """
    Fetch all access rights for a user in a specific entity context.
    context is a context class like:
    {'organization_id': id, 'project_id': id, 'branch_id': id}
    """

    # Query all active roles assigned to user in the context
    stmt = (
        select(AccessRight.entry)
        .join(RoleAccessRight)
        .join(Role)
        .join(Role)
        .join(RoleUserLink)
        .where(Role.is_active, RoleUserLink.user_id == user_id)
    )

    # Apply context filters if sub != "*" and sub != req_sub:
    if context.organization_id is not None:
        stmt = stmt.where(RoleUserLink.organization_id == context.organization_id)
    if context.project_id is not None:
        stmt = stmt.where(RoleUserLink.project_id == context.project_id)
    if context.branch_id is not None:
        stmt = stmt.where(RoleUserLink.branch_id == context.branch_id)
    if context.env_type is not None:
        stmt = stmt.where(RoleUserLink.env_type == context.env_type)

    result = await session.execute(stmt)
    return list(result.scalars().all())


async def check_access(
    session: AsyncSession, user_id, required_access: str, context: PermissionCheckContextPublic
) -> bool:
    rights = await get_user_rights(session, user_id, context)
    return match_access(required_access, rights)
