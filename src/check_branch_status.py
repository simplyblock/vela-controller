import logging

from sqlmodel.ext.asyncio.session import AsyncSession

from ._util import Identifier
from .api.organization.project.branch import refresh_branch_status
from .models.branch import Branch, BranchServiceStatus

logger = logging.getLogger(__name__)


async def _resolve_branch_status(session: AsyncSession, branch_id: Identifier) -> BranchServiceStatus:
    branch = await session.get(Branch, branch_id)
    if branch is None:
        logger.warning("Branch %s not found while resolving status; returning UNKNOWN", branch_id)
        return BranchServiceStatus.UNKNOWN
    status = branch.status or BranchServiceStatus.UNKNOWN
    if isinstance(status, BranchServiceStatus):
        return status
    member = BranchServiceStatus._value2member_map_.get(status) if status else None
    if member is None:
        logger.warning("Encountered unknown branch status %s; returning UNKNOWN", status)
        return BranchServiceStatus.UNKNOWN
    return member


async def get_branch_status(
    branch_id: Identifier,
    *,
    session: AsyncSession | None = None,
) -> BranchServiceStatus:
    if session is not None:
        return await _resolve_branch_status(session, branch_id)

    return await refresh_branch_status(branch_id)
