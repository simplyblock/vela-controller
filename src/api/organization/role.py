from collections.abc import Sequence
from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from ...models._util import Identifier
from ...models.role import (
    AccessRight,
    AccessRightPublic,
    PermissionAccessCheckPublic,
    PermissionCheckContextPublic,
    Role,
    RoleAccessRight,
    RoleAssignmentPublic,
    RoleCreate,
    RoleDeletePublic,
    RolePublic,
    RoleType,
    RoleTypePublic,
    RoleUnassignmentPublic,
    RoleUpdate,
    RoleUserLink,
    RoleUserLinkPublic,
    RoleWithPermissionsPublic,
)
from ..access_right_utils import check_access
from ..auth import authenticated_user
from ..db import SessionDep
from ..dependencies import OrganizationDep, RoleDep

api = APIRouter(dependencies=[Depends(authenticated_user)], tags=["role"])


async def lookup_access_rights(session: AsyncSession, entries: list[AccessRightPublic]) -> Sequence[AccessRight]:
    statement = select(AccessRight).where(AccessRight.entry.in_(entries))  # type: ignore[attr-defined]
    access_rights = (await session.exec(statement)).all()

    if missing_entries := {str(entry) for entry in entries} - {ar.entry for ar in access_rights}:
        raise HTTPException(
            status_code=404,
            detail=f"AccessRight entries not found: {', '.join(sorted(missing_entries))}",
        )

    return access_rights


class AccessCheckRequest(BaseModel):
    access: str  # e.g., "project:settings:update"
    project_id: Identifier | None = None
    branch_id: Identifier | None = None
    env_type: str | None = None


class RoleAssignmentPayload(BaseModel):
    # Single or multiple projects/branches/environments
    project_ids: list[Identifier] = []
    branch_ids: list[Identifier] = []
    env_types: list[str] = []


@api.post("/")
async def create_role(
    session: SessionDep,
    organization_id: Identifier,
    payload: RoleCreate,
) -> RolePublic:
    role = Role(
        role_type=RoleType[payload.role_type],
        is_active=payload.is_active,
        is_deletable=payload.is_deletable,
        name=payload.name,
        description=payload.description,
        access_rights=[
            RoleAccessRight(
                organization_id=organization_id,
                access_right_id=access_right.id,
            )
            for access_right in await lookup_access_rights(session, payload.access_rights)
        ]
        if payload.access_rights is not None
        else [],
    )
    role.organization_id = organization_id
    session.add(role)
    await session.commit()
    await session.refresh(role)

    return RolePublic(
        id=role.id,
        organization_id=role.organization_id,
        name=role.name,
        role_type=role.role_type.name,  # type: ignore[arg-type]
        is_active=role.is_active,
        is_deletable=role.is_deletable,
        description=role.description,
        user_count=len(await role.awaitable_attrs.users),
    )


@api.post("/check_access/{user_id}/")
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


@api.get("/")
async def list_roles(
    session: SessionDep,
    organization: OrganizationDep,
) -> list[RoleWithPermissionsPublic]:
    """
    List all roles and their access rights within an organization
    """

    # Include access rights in response
    async def to_api_role(role: Role) -> RoleWithPermissionsPublic:
        result = await session.execute(
            select(AccessRight.entry)
            .select_from(RoleAccessRight)  # <- explicitly say the left table
            .join(AccessRight)
            .where(RoleAccessRight.organization_id == role.organization_id, RoleAccessRight.role_id == role.id)
        )
        count = len(await role.awaitable_attrs.users)
        return RoleWithPermissionsPublic(
            id=role.id,
            organization_id=role.organization_id,
            description=role.description,
            is_deletable=role.is_deletable,
            role_type=cast("RoleTypePublic", role.role_type.name),
            name=role.name,
            is_active=role.is_active,
            access_rights=[row.entry for row in result.all()],
            user_count=count,
        )

    return [await to_api_role(role) for role in await organization.awaitable_attrs.roles]


@api.get("/role-assignments/")
async def list_role_assignments(
    session: SessionDep,
    organization_id: Identifier,
    user_id: UUID | None = None,
) -> list[RoleUserLinkPublic]:
    """
    List role-user assignments within an organization.
    Optionally filter by user_id.
    """
    stmt = select(RoleUserLink).where(RoleUserLink.organization_id == organization_id)
    if user_id:
        stmt = stmt.where(RoleUserLink.user_id == user_id)

    result = await session.execute(stmt)

    return [
        RoleUserLinkPublic(
            organization_id=link.organization_id,
            project_id=link.project_id,
            branch_id=link.branch_id,
            role_id=link.role_id,
            user_id=link.user_id,
            env_types=link.env_types,
        )
        for link in result.scalars().all()
    ]


instance_api = APIRouter(prefix="/{role_id}")


@instance_api.put("/")
async def modify_role(
    session: SessionDep,
    organization_id: Identifier,
    role: RoleDep,
    payload: RoleUpdate,
) -> RolePublic:
    if not role.is_deletable:
        raise HTTPException(403, "Role cannot be modified")

    role.is_active = payload.is_active
    role.name = payload.name
    role.description = payload.description

    if payload.access_rights is not None:
        await role.awaitable_attrs.access_rights  # Ensure access_rights are loaded
        role.access_rights = [
            RoleAccessRight(
                organization_id=organization_id,  # FIXME remove redundant field
                access_right_id=access_right.id,
            )
            for access_right in await lookup_access_rights(session, payload.access_rights)
        ]

    await session.commit()
    await session.refresh(role)

    return RolePublic(
        id=role.id,
        organization_id=role.organization_id,  # type: ignore[arg-type]
        name=role.name,
        role_type=role.role_type.name,  # type: ignore[arg-type]
        is_active=role.is_active,
        is_deletable=role.is_deletable,
        description=role.description,
        user_count=len(await role.awaitable_attrs.users),
    )


@instance_api.delete("/")
async def delete_role(
    session: SessionDep,
    role: RoleDep,
) -> RoleDeletePublic:
    await session.delete(role)
    await session.commit()
    return RoleDeletePublic(status="deleted")


@instance_api.post("/assign/{user_id}/")
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
    created_links = []

    # Create RoleUserLink for every combination
    if (
        (payload.project_ids and role.role_type != RoleType.project)
        or (payload.env_types and role.role_type != RoleType.environment)
        or (payload.branch_ids and role.role_type != RoleType.branch)
        or (
            not payload.project_ids
            and not payload.env_types
            and not payload.branch_ids
            and role.role_type != RoleType.organization
        )
    ):
        raise HTTPException(
            422,
            f"Role type {role.role_type.name} does not match entities: {payload.project_ids}, {payload.branch_ids}, {
                payload.env_types
            }",
        )

    if role.role_type == RoleType.organization:
        link = RoleUserLink(organization_id=organization.id, role_id=role.id, user_id=user_id)
        session.add(link)
        created_links.append(link)

    for project_id in payload.project_ids:
        link = RoleUserLink(organization_id=organization.id, role_id=role.id, user_id=user_id, project_id=project_id)
        session.add(link)
        created_links.append(link)

    if payload.env_types:
        stmt = select(RoleUserLink).where(
            RoleUserLink.organization_id == organization.id,
            RoleUserLink.role_id == role.id,
            RoleUserLink.user_id == user_id,
        )
        result = await session.execute(stmt)
        env_link = result.scalar_one_or_none()  # type: RoleUserLink | None
        if env_link is None:
            env_link = RoleUserLink(
                organization_id=organization.id,
                role_id=role.id,
                user_id=user_id,
                env_types=payload.env_types,
            )
            session.add(env_link)
        else:
            existing = env_link.env_types or []
            env_link.env_types = list(dict.fromkeys(existing + payload.env_types))
        created_links.append(env_link)

    for branch_id in payload.branch_ids:
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
            env_types=link.env_types,
        )
        for link in created_links
    ]

    return RoleAssignmentPublic(status="assigned", count=len(created_links), links=result_links)


@instance_api.post("/unassign/{user_id}/")
async def unassign_role(
    session: SessionDep,
    role: RoleDep,
    organization: OrganizationDep,
    user_id: UUID,
    context: dict[str, UUID] | None = None,
) -> RoleUnassignmentPublic:
    if not role.is_deletable:
        raise HTTPException(403, "Role cannot be unassigned")

    """
    Remove a role assignment for a user in a specific context.
    If context is None, remove all assignments of this role for the user.
    """
    stmt = select(RoleUserLink).where(
        RoleUserLink.role_id == role.id,
        RoleUserLink.organization_id == organization.id,
        RoleUserLink.user_id == user_id,
    )

    if context:
        for key, val in context.items():
            if hasattr(RoleUserLink, key):
                stmt = stmt.where(getattr(RoleUserLink, key) == val)

    result = await session.execute(stmt)
    links = result.scalars().all()

    if not links:
        raise HTTPException(404, "No matching role assignment found")

    for link in links:
        await session.delete(link)

    await session.commit()
    return RoleUnassignmentPublic(status="unassigned", count=len(links))


api.include_router(instance_api)
