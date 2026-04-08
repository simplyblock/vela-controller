"""Celery sub-tasks for async branch deletion (deployment layer).

These tasks handle the infrastructure-level teardown steps that can run in
parallel: K8s namespace/VM deletion, Keycloak realm removal, and backup
snapshot cleanup.  The API layer's ``delete_tasks.finalize_delete`` chord
callback then removes the DB record after all sub-tasks have settled.
"""

import logging

from asgiref.sync import async_to_sync
from keycloak.exceptions import KeycloakError
from ulid import ULID

from ..api._util.backups import delete_branch_backups
from ..api.keycloak import realm_admin
from ..database import AsyncSessionLocal
from ..worker import app
from . import delete_deployment

logger = logging.getLogger(__name__)


async def _delete_keycloak(branch_id: ULID) -> None:
    try:
        await realm_admin("master").a_delete_realm(str(branch_id))
    except KeycloakError as exc:
        if getattr(exc, "response_code", None) == 404:
            logger.warning("Keycloak realm not found for branch %s; skipping.", branch_id)
        else:
            raise


async def _delete_snapshots(branch_id: ULID) -> None:
    async with AsyncSessionLocal() as session:
        await delete_branch_backups(session, branch_id)


@app.task(name="simplyblock.vela.deployment.delete.k8s")
def delete_k8s_deployment(branch_id: str) -> None:
    """Delete the K8s namespace and associated VM for a branch."""
    async_to_sync(delete_deployment)(ULID.from_str(branch_id))


@app.task(name="simplyblock.vela.deployment.delete.keycloak")
def delete_keycloak_realm(branch_id: str) -> None:
    """Delete the Keycloak realm for a branch (404 is treated as success)."""
    async_to_sync(_delete_keycloak)(ULID.from_str(branch_id))


@app.task(name="simplyblock.vela.deployment.delete.backups")
def delete_backup_snapshots(branch_id: str) -> None:
    """Delete K8s volume snapshots for all backups belonging to a branch."""
    async_to_sync(_delete_snapshots)(ULID.from_str(branch_id))
