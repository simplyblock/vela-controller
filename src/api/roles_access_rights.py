from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from uuid import UUID
from typing import Optional, Dict

from .models.role import Role, RoleUserLink, AccessRight
from .db import get_db
from .access_right_utils import check_access

router = APIRouter(prefix="/roles")

from pydantic import BaseModel
from typing import Optional
from uuid import UUID

class AccessCheckRequest(BaseModel):
    access: str  # e.g., "project:settings:update"
    organization_id: Optional[UUID] = None
    project_id: Optional[UUID] = None
    branch_id: Optional[UUID] = None
    environment_id: Optional[UUID] = None

from typing import List

class AccessRightPayload(BaseModel):
    entry: str  # e.g., "project:settings:update"

class RolePayload(BaseModel):
    role_type: str
    is_active: bool = True
    access_rights: Optional[List[AccessRightPayload]] = []

class RoleAssignmentPayload(BaseModel):
    # Single or multiple projects/branches/environments
    organization_id: Optional[UUID] = None
    project_ids: Optional[List[UUID]] = None
    branch_ids: Optional[List[UUID]] = None
    environment_ids: Optional[List[UUID]] = None

# ----------------------
# Create role
# ----------------------
@router.post("/")
async def create_role(payload: RolePayload, session: AsyncSession = Depends(get_db)):
    role = Role(role_type=payload.role_type, is_active=payload.is_active)
    session.add(role)
    await session.commit()
    await session.refresh(role)

    # Add access rights if provided
    if payload.access_rights:
        for ar_payload in payload.access_rights:
            ar = AccessRight(entry=ar_payload.entry)
            role.access_rights.append(ar)
        session.add(role)
        await session.commit()
        await session.refresh(role)

    return role

# ----------------------
# Modify role
# ----------------------
@router.put("/{role_id}")
async def modify_role(role_id: UUID, payload: RolePayload, session: AsyncSession = Depends(get_db)):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(404, f"Role {role_id} not found")

    role.role_type = payload.role_type
    role.is_active = payload.is_active

    if payload.access_rights is not None:
        # Clear existing access rights and add new ones
        role.access_rights.clear()
        for ar_payload in payload.access_rights:
            ar = AccessRight(entry=ar_payload.entry)
            role.access_rights.append(ar)

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
    payload: RoleAssignmentPayload,
    session: AsyncSession = Depends(get_db)
):
    """
    Assign a role to a user in one or more contexts. The context is passed as JSON.
    """
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(404, f"Role {role_id} not found")

    # Prepare combinations of context assignments
    org_id = payload.organization_id
    project_ids = payload.project_ids or [None]
    branch_ids = payload.branch_ids or [None]
    env_ids = payload.environment_ids or [None]

    created_links = []

    # Create RoleUserLink for every combination
    for project_id in project_ids:
        for branch_id in branch_ids:
            for env_id in env_ids:
                link = RoleUserLink(role_id=role_id, user_id=user_id)
                if org_id:
                    link.organization_id = org_id
                if project_id:
                    link.project_id = project_id
                if branch_id:
                    link.branch_id = branch_id
                if env_id:
                    link.environment_id = env_id

                session.add(link)
                created_links.append(link)

    await session.commit()

    # Refresh all links
    for link in created_links:
        await session.refresh(link)

    return {"status": "assigned", "count": len(created_links), "links": created_links}



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
        RoleUserLink.user_id == user_id,
    )

    if context:
        for key, val in context.items():
            if hasattr(RoleUserLink, key):
                stmt = stmt.where(getattr(RoleUserLink, key) == val)

    # ✅ Use session.execute() instead of session.exec()
    result = await session.execute(stmt)
    links = result.scalars().all()

    if not links:
        raise HTTPException(404, "No matching role assignment found")

    for link in links:
        await session.delete(link)

    await session.commit()
    return {"status": "unassigned", "count": len(links)}


# ----------------------
# Check access for a user
# ----------------------
from fastapi import Body

@router.post("/check_access/{user_id}")
async def api_check_access(
    user_id: UUID,
    payload: AccessCheckRequest = Body(...),
    session: AsyncSession = Depends(get_db),
):
    """
    Example POST JSON:
    {
        "access": "project:settings:update",
        "project_id": "01ABCDEF2345XYZ"
    }
    """
    # Build entity_context from the JSON payload
    entity_context = {
        k: v
        for k, v in {
            "organization_id": payload.organization_id,
            "project_id": payload.project_id,
            "branch_id": payload.branch_id,
            "environment_id": payload.environment_id,
        }.items()
        if v is not None
    }

    allowed = await check_access(session, user_id, payload.access, entity_context)
    if not allowed:
        raise HTTPException(status_code=403, detail="Access denied")

    return {"access_granted": True, "context": entity_context}

