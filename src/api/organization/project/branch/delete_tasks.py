"""Celery chord tasks for async branch deletion.

The chord dispatches three sub-tasks in parallel (K8s, Keycloak, backups) and
calls ``finalize_delete`` when all sub-tasks have settled.  With
``task_chord_propagates = False`` the callback always fires, so the branch DB
record is removed regardless of individual sub-task outcomes (errors are logged
but do not abort the delete).
"""

import logging
from uuid import UUID

from asgiref.sync import async_to_sync
from celery import chord
from ulid import ULID

from .....database import AsyncSessionLocal
from .....deployment.delete import delete_backup_snapshots, delete_k8s_deployment, delete_keycloak_realm
from .....models.branch import Branch
from .....worker import app
from ...._util.resourcelimit import delete_branch_provisioning

logger = logging.getLogger(__name__)


async def _async_finalize_delete(job_results: list, branch_id: str) -> dict:
    errors: list[str] = []
    for result in job_results:
        if isinstance(result, Exception):
            errors.append(str(result))
            logger.error("Delete sub-task error for branch %s: %s", branch_id, result)

    branch_ulid = ULID.from_str(branch_id)
    async with AsyncSessionLocal() as session:
        branch = await session.get(Branch, branch_ulid)
        if branch is None:
            logger.warning("Branch %s not found in finalize_delete; already deleted.", branch_id)
            return {"errors": errors}

        await delete_branch_provisioning(session, branch_ulid, commit=False)
        await session.delete(branch)
        await session.commit()

    return {"errors": errors}


@app.task(name="simplyblock.vela.branch.delete.finalize")
def finalize_delete(job_results: list, branch_id: str) -> dict:
    """Chord callback: delete provisioning records and the branch DB row."""
    return async_to_sync(_async_finalize_delete)(job_results, branch_id)


def dispatch_delete(branch_id: str) -> UUID:
    """Dispatch the delete chord; return the chord result UUID."""
    jobs = [
        delete_k8s_deployment.s(branch_id),
        delete_keycloak_realm.s(branch_id),
        delete_backup_snapshots.s(branch_id),
    ]
    result = chord(jobs)(finalize_delete.s(branch_id=branch_id))
    return UUID(result.id)
