from fastapi import APIRouter, Depends, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from uuid import UUID
from typing import Optional, Dict

from .models.role import Role, AccessRight, RoleUserLink, RoleAccessRight
from .db import get_db
from .access_right_utils import check_access, get_user_rights

router = APIRouter(prefix="/roles")


# ----------------------
# Create role
# ----------------------
@router.post("/")
async def create_role(role: Role, session: AsyncSession = Depends(get_db)):
    session.add(role)
    await session.commit()
    await session.refresh(role)
    return role


# ----------------------
# Modify role
# ----------------------
@router.put("/{role_id}")
async def modify_role(role_id: UUID, updated_role: Role, session: AsyncSession = Depends(get_db)):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(404, f"Role {role_id} not found")

    role.role_type = updated_role.role_type
    role.is_active = updated_role.is_active
    # Optionally update access rights
    if updated_role.access_rights:
        # Clear existing and add new
        role.access_rights.clear()
        role.access_rights.extend(updated_role.access_rights)

    session.add(role)
    await session.commit()
    await session.refresh(role)
    return role


# ----------------------
# Delete role
# ----------------------
@router.delete("/{role_id}")
async def delete_role(role_id: UUID, session: AsyncSession = Depends(get_db)):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(404, f"Role {role_id} not found")
    await session.delete(role)
    await session.commit()
    return {"status": "deleted"}


# ----------------------
# Assign role to user (with context)
# ----------------------
@router.post("/{role_id}/assign/{user_id}")
async def assign_role(
    role_id: UUID,
    user_id: UUID,
    context: Optional[Dict[str, UUID]] = None,
    session: AsyncSession = Depends(get_db)
):
    """
    context: dict with keys like organization_id, project_id, branch_id, environment_id
    """
    # Check role exists
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(404, f"Role {role_id} not found")

    link = RoleUserLink(role_id=role_id, user_id=user_id)
    if context:
        for key, val in context.items():
            setattr(link, f"{key}_entity", val)

    session.add(link)
    await session.commit()
    await session.refresh(link)
    return {"status": "assigned", "link": link}


# ----------------------
# Unassign role from user (with context)
# ----------------------
@router.post("/{role_id}/unassign/{user_id}")
async def unassign_role(
    role_id: UUID,
    user_id: UUID,
    context: Optional[Dict[str, UUID]] = None,
    session: AsyncSession = Depends(get_db)
):
    """
    Remove a role assignment for a user in a specific context.
    If context is None, remove all assignments of this role for the user.
    """
    stmt = select(RoleUserLink).where(
        RoleUserLink.role_id == role_id,
        RoleUserLink.user_id == user_id
    )

    if context:
        for key, val in context.items():
            stmt = stmt.where(getattr(RoleUserLink, f"{key}_entity") == val)

    result = await session.exec(stmt)
    links = result.all()
    if not links:
        raise HTTPException(404, "No matching role assignment found")

    for link in links:
        await session.delete(link)

    await session.commit()
    return {"status": "unassigned", "count": len(links)}


# ----------------------
# Check access for a user
# ----------------------

@router.get("/check_access/{user_id}")
async def api_check_access(user_id: UUID, access: str, context: Dict[str, UUID], session: AsyncSession = Depends(get_db)):
    allowed = await check_access(session, user_id, access, context)
    if not allowed:
        raise HTTPException(status_code=403, detail="Access denied")
    return {"access_granted": allowed}
