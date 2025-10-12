from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select


# Wildcard matcher
def match_access(required: str, rights: List[str]) -> bool:
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

from typing import List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

async def get_user_rights(session: AsyncSession, user_id, entity_context) -> List[str]:
    """
    Fetch all access rights for a user in a specific entity context.
    entity_context is a dict like:
    {'organization_id': id, 'project_id': id, 'branch_id': id}
    """
    from .models.role import RoleUserLink, RoleAccessRight, AccessRight, Role

    # Query all active roles assigned to user in the context
    stmt = (
        select(AccessRight.entry)
        .join(RoleAccessRight, AccessRight.id == RoleAccessRight.access_right_id)
        .join(Role, Role.id == RoleAccessRight.role_id)
        .join(RoleUserLink, RoleUserLink.role_id == Role.id)
        .where(
            Role.is_active == True,
            RoleUserLink.user_id == user_id
        )
    )

    # Apply context filters if sub != "*" and sub != req_sub:
    if "organization_id" in entity_context:
        stmt = stmt.where(RoleUserLink.organization_id == entity_context["organization_id"])
    if "project_id" in entity_context:
        stmt = stmt.where(RoleUserLink.project_entity == entity_context["project_id"])
    if "branch_id" in entity_context:
        stmt = stmt.where(RoleUserLink.branch_entity == entity_context["branch_id"])
    if "environment_id" in entity_context:
        stmt = stmt.where(RoleUserLink.environment_entity == entity_context["environment_id"])

    result = await session.execute(stmt)
    return [r for r in result.scalars().all()]

async def check_access(session: AsyncSession, user_id, required_access: str, entity_context) -> bool:
    rights = await get_user_rights(session, user_id, entity_context)
    return match_access(required_access, rights)

