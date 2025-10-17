from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlmodel import and_, select

from .access_right_utils import check_access
from .db import SessionDep
from .models._util import Identifier
from .models.membership import Membership
from .models.organization import OrganizationDep
from .models.role import (
    AccessRight,
    PermissionAccessCheckPublic,
    PermissionCheckContextPublic,
    Role,
    RoleAccessRight,
    RoleAssignmentPublic,
    RoleAssignmentsPublic,
    RoleDeletePublic,
    RoleDep,
    RolePermissionAssignmentPublic,
    RolePermissionsPublic,
    RolePublic,
    RoleType,
    RoleTypePublic,
    RoleUnassignmentPublic,
    RoleUserLink,
    RoleUserLinkPublic,
    UserPermissionPublic,
)
from .models.user import User

router = APIRouter(tags=["role"])


class AccessCheckRequest(BaseModel):
    access: str  # e.g., "project:settings:update"
    project_id: Identifier | None = None
    branch_id: Identifier | None = None
    env_type: str | None = None


class RolePayload(BaseModel):
    role_id: Identifier
    name: str
    role_type: RoleTypePublic
    is_active: bool = True
    access_rights: list[str] | None = []


class RolePayloadUpdate(BaseModel):
    name: str
    role_type: RoleTypePublic
    is_active: bool = True
    access_rights: list[str] | None = []


class RoleAssignmentPayload(BaseModel):
    # Single or multiple projects/branches/environments
    project_ids: list[Identifier] | None = None
    branch_ids: list[Identifier] | None = None
    env_types: list[str] | None = None


# ----------------------
# Create role
# ----------------------
@router.post("/organizations/{organization_id}/roles/")
async def create_role(
    session: SessionDep,
    organization_id: Identifier,
    payload: RolePayload,
) -> RolePublic:
    role = Role(role_type=RoleType(payload.role_type), is_active=payload.is_active, name=payload.name)
    role.organization_id = organization_id
    session.add(role)
    await session.commit()
    await session.refresh(role)

    # Add access rights if provided
    if payload.access_rights:
        for ar_payload in payload.access_rights:
            stmt = select(AccessRight).where(AccessRight.entry == ar_payload)
            result = await session.execute(stmt)
            ar = result.scalar_one_or_none()
            role_access_right = RoleAccessRight(
                organization_id=organization_id, role_id=role.id, access_right_id=ar.id if ar is not None else None
            )
            session.add(role_access_right)
        await session.commit()
        await session.refresh(role)

    return RolePublic(
        id=role.id,
        organization_id=role.organization_id,
        name=role.name,
        role_type=role.role_type.name,  # type: ignore[arg-type]
        is_active=role.is_active,
    )


# ----------------------
# Modify role
# ----------------------
@router.put("/organizations/{organization_id}/roles/{role_id}/")
async def modify_role(
    session: SessionDep,
    organization_id: Identifier,
    role: RoleDep,
    payload: RolePayloadUpdate,
) -> RolePublic:
    role.is_active = payload.is_active
    role.name = payload.name

    if payload.access_rights is not None:
        # Clear existing access rights and add new ones

        stmt = select(RoleAccessRight).where(
            RoleAccessRight.role_id == role.id, RoleAccessRight.organization_id == organization_id
        )
        result = await session.execute(stmt)
        ar = result.scalars().all()
        if ar:
            for a in ar:
                await session.delete(a)
            await session.commit()
        for ar_payload in payload.access_rights:
            stmt2 = select(AccessRight).where(AccessRight.entry == ar_payload)
            result2 = await session.execute(stmt2)
            ar2 = result2.scalar_one()
            role_access_right = RoleAccessRight(
                organization_id=organization_id, role_id=role.id, access_right_id=ar2.id
            )
            session.add(role_access_right)
        await session.commit()
        await session.refresh(role)

    session.add(role)
    await session.commit()
    await session.refresh(role)
    return RolePublic(
        id=role.id,
        organization_id=role.organization_id,  # type: ignore[arg-type]
        name=role.name,
        role_type=role.role_type,  # type: ignore[arg-type]
        is_active=role.is_active,
    )


# ----------------------
# Delete role
# ----------------------
@router.delete("/organizations/{organization_id}/roles/{role_id}/")
async def delete_role(
    session: SessionDep,
    organization_id: Identifier,
    role_id: Identifier,
) -> RoleDeletePublic:
    stmt = select(Role).where(Role.id == role_id, Role.organization_id == organization_id)
    result = await session.execute(stmt)
    role = result.scalar_one_or_none()
    if not role:
        raise HTTPException(404, f"Role {role_id} not found")

    await session.delete(role)
    await session.commit()
    return RoleDeletePublic(status="deleted")


# ----------------------
# Assign role to user (with context)
# ----------------------
@router.post("/organizations/{organization_id}/roles/{role_id}/assign/{user_id}/")
async def assign_role(
    session: SessionDep,
    organization: OrganizationDep,
    role: RoleDep,
    user_id: UUID,
    payload: RoleAssignmentPayload,
) -> RoleAssignmentPublic:
    """
    Assign a role to a user in one or more contexts. The context is passed as JSON.
    """
    # Prepare combinations of context assignments
    project_ids = payload.project_ids or None
    branch_ids = payload.branch_ids or None
    env_types = payload.env_types or None

    created_links = []

    # Create RoleUserLink for every combination
    def has_values(lst):
        return lst is not None and any(x is not None for x in lst)

    if (
        (has_values(project_ids) and int(role.role_type.value) != 2)
        or (has_values(env_types) and int(role.role_type.value) != 1)
        or (has_values(branch_ids) and int(role.role_type.value) != 3)
        or (
            not has_values(project_ids)
            and not has_values(env_types)
            and not has_values(branch_ids)
            and int(role.role_type.value) != 0
        )
    ):
        raise HTTPException(
            422, f"Role type {role.role_type.name} does not match entities: {project_ids}, {branch_ids}, {env_types}"
        )

    if project_ids:
        for project_id in project_ids:
            link = RoleUserLink(
                organization_id=organization.id, role_id=role.id, user_id=user_id, project_id=project_id
            )
            session.add(link)
            created_links.append(link)

    if env_types:
        for env_type in env_types:
            link = RoleUserLink(organization_id=organization.id, role_id=role.id, user_id=user_id, env_type=env_type)
            session.add(link)
            created_links.append(link)

    if branch_ids:
        for branch_id in branch_ids:
            link = RoleUserLink(organization_id=organization.id, role_id=role.id, user_id=user_id, branch_id=branch_id)
            session.add(link)
            created_links.append(link)

    await session.commit()

    # Refresh all links
    for link in created_links:
        await session.refresh(link)

    result_links = [
        RoleUserLinkPublic(
            organization_id=link.organization_id,
            project_id=link.project_id,
            branch_id=link.branch_id,
            role_id=link.role_id,
            user_id=link.user_id,
            env_type=link.env_type,
        )
        for link in created_links
    ]

    return RoleAssignmentPublic(status="assigned", count=len(created_links), links=result_links)


# ----------------------
# Unassign role from user (with context)
# ----------------------
@router.post("/organizations/{organization_id}/roles/{role_id}/unassign/{user_id}/")
async def unassign_role(
    session: SessionDep,
    role_id: Identifier,
    organization_id: Identifier,
    user_id: UUID,
    context: dict[str, UUID] | None = None,
) -> RoleUnassignmentPublic:
    """
    Remove a role assignment for a user in a specific context.
    If context is None, remove all assignments of this role for the user.
    """
    stmt = select(RoleUserLink).where(
        RoleUserLink.role_id == role_id,
        RoleUserLink.organization_id == organization_id,
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
    return RoleUnassignmentPublic(status="unassigned", count=len(links))


# ----------------------
# Check access for a user
# ----------------------
@router.post("/organizations/{organization_id}/roles/check_access/{user_id}/")
async def api_check_access(
    session: SessionDep,
    organization_id: Identifier,
    user_id: UUID,
    payload: AccessCheckRequest,
) -> PermissionAccessCheckPublic:
    """
    Example POST JSON:
    {
        "access": "project:settings:update",
        "project_id": "01ABCDEF2345XYZ"
    }
    """
    # Build entity_context from the JSON payload
    context = PermissionCheckContextPublic(
        organization_id=organization_id,
        project_id=payload.project_id,
        branch_id=payload.branch_id,
        env_type=payload.env_type,
    )

    allowed = await check_access(session, user_id, payload.access, context)
    if not allowed:
        raise HTTPException(status_code=403, detail="Access denied")

    return PermissionAccessCheckPublic(access_granted=True, context=context)


@router.get("/organizations/{organization_id}/roles/")
async def list_roles(
    session: SessionDep,
    organization_id: Identifier,
) -> RolePermissionsPublic:
    """
    List all roles and their access rights within an organization
    """
    stmt = select(Role).where(Role.organization_id == organization_id)
    result = await session.execute(stmt)
    roles = result.scalars().all()

    # Include access rights in response
    role_list: list[RolePermissionAssignmentPublic] = []
    for role in roles:
        stmt2 = (
            select(AccessRight.entry)
            .select_from(RoleAccessRight)  # <- explicitly say the left table
            .join(AccessRight)
            .where(RoleAccessRight.organization_id == organization_id, RoleAccessRight.role_id == role.id)
        )
        result2 = await session.execute(stmt2)
        rows = result2.all()
        role_list.append(
            RolePermissionAssignmentPublic(
                role_id=role.id,
                role_type=role.role_type,
                name=role.name,
                is_active=role.is_active,
                access_rights=[ar[0] for ar in rows],
            )
        )
    return RolePermissionsPublic(count=len(role_list), links=role_list)


@router.get("/organizations/{organization_id}/roles/role-assignments/")
async def list_role_assignments(
    session: SessionDep,
    organization_id: Identifier,
    user_id: UUID | None = None,
) -> RoleAssignmentsPublic:
    """
    List role-user assignments within an organization.
    Optionally filter by user_id.
    """
    stmt = select(RoleUserLink).where(RoleUserLink.organization_id == organization_id)
    if user_id:
        stmt = stmt.where(RoleUserLink.user_id == user_id)

    result = await session.execute(stmt)
    links = result.scalars().all()

    assignments = [
        RoleUserLinkPublic(
            organization_id=link.organization_id,
            project_id=link.project_id,
            branch_id=link.branch_id,
            role_id=link.role_id,
            user_id=link.user_id,
            env_type=link.env_type,
        )
        for link in links
    ]

    return RoleAssignmentsPublic(count=len(assignments), links=assignments)


@router.get("/users/{user_id}/permissions/")
async def list_user_permissions(
    session: SessionDep,
    user_id: UUID,
) -> list[UserPermissionPublic]:
    stmt = (
        select(  # type: ignore[call-overload]
            AccessRight.entry,
            RoleUserLink.organization_id,
            RoleUserLink.project_id,
            RoleUserLink.branch_id,
            RoleUserLink.env_type,
        )
        .select_from(AccessRight)
        .join(User, User.id == user_id)
        .join(Membership, Membership.user_id == User.id)
        .join(RoleAccessRight, AccessRight.id == RoleAccessRight.access_right_id)
        .join(Role, Role.id == RoleAccessRight.role_id)
        .join(RoleUserLink, and_(RoleUserLink.role_id == Role.id, RoleUserLink.user_id == User.id))
        .where(Role.is_active)
    )

    result = await session.execute(stmt)

    def is_organization_level_permission(row):
        any(getattr(row, attr) is not None for attr in ["project_id", "branch_id", "env_type"])

    return [
        UserPermissionPublic(
            permission=row.entry,
            organization_id=row.organization_id if is_organization_level_permission(row) else None,
            project_id=row.project_id,
            branch_id=row.branch_id,
            env_type=row.env_type,
        )
        for row in result.all()
    ]


@router.get("/system/available-permissions/")
async def list_available_permissions(
    session: SessionDep,
) -> list[str]:
    """
    List all access rights defined in the system.
    """
    stmt = select(AccessRight.entry)
    result = await session.execute(stmt)
    entries = [row[0] for row in result.all()]
    return entries
