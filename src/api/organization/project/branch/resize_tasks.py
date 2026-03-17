"""Celery chord tasks for proactive branch resize completion.

The chord dispatches one sub-task per resource type (in the deployment layer)
and calls ``finalize_resize`` when all sub-tasks have settled.  With
``task_chord_propagates = False`` the callback always fires, even on partial
failure, so succeeded fields are persisted and ``resize_task_id`` is cleared
regardless of individual sub-task outcomes.
"""

import logging
from uuid import UUID

from asgiref.sync import async_to_sync
from celery import chord
from ulid import ULID

from .....database import AsyncSessionLocal
from .....deployment.health import collect_branch_service_health, derive_branch_status_from_services
from .....deployment.resize import resize_cpu_memory, resize_database_pvc, resize_iops, resize_storage_pvc
from .....models.branch import Branch, BranchServiceStatus
from .....models.resources import ResourceLimitsPublic
from .....worker import app
from ...._util.resourcelimit import create_or_update_branch_provisioning

logger = logging.getLogger(__name__)

_FIELD_TO_BRANCH_ATTR: dict[str, str] = {
    "database_size": "database_size",
    "storage_size": "storage_size",
    "milli_vcpu": "milli_vcpu",
    "memory_bytes": "memory",
    "iops": "iops",
}

_FIELD_TO_RESOURCE_LIMITS_KWARG: dict[str, str] = {
    "database_size": "database_size",
    "storage_size": "storage_size",
    "milli_vcpu": "milli_vcpu",
    "memory_bytes": "ram",
    "iops": "iops",
}


async def _apply_succeeded_fields(session: object, branch: Branch, succeeded: dict[str, int]) -> None:
    """Write each succeeded resize field onto the branch and update provisioning records."""
    for field, value in succeeded.items():
        branch_attr = _FIELD_TO_BRANCH_ATTR.get(field)
        if branch_attr is None:
            continue
        if getattr(branch, branch_attr) == value:
            continue
        setattr(branch, branch_attr, value)
        limits_kwarg = _FIELD_TO_RESOURCE_LIMITS_KWARG[field]
        await create_or_update_branch_provisioning(
            session,  # type: ignore[arg-type]
            branch,
            ResourceLimitsPublic(**{limits_kwarg: value}),
            commit=False,
        )


async def _async_finalize_resize(
    job_results: list,
    branch_id: str,
    effective_parameters: dict,
    job_fields: list[list[str]],
) -> dict:
    succeeded: dict[str, int] = {}
    errors: list[str] = []

    for result, fields in zip(job_results, job_fields, strict=False):
        if isinstance(result, Exception):
            errors.append(str(result))
        else:
            for field in fields:
                if field in effective_parameters:
                    succeeded[field] = effective_parameters[field]

    async with AsyncSessionLocal() as session:
        branch = await session.get(Branch, ULID.from_str(branch_id))
        if branch is None:
            logger.error("Branch %s not found during finalize_resize", branch_id)
            return {**succeeded, "errors": errors}

        await _apply_succeeded_fields(session, branch, succeeded)

        service_status = await collect_branch_service_health(branch.id)
        if branch.status == BranchServiceStatus.RESIZING:
            branch.set_status(
                derive_branch_status_from_services(service_status, storage_enabled=branch.enable_file_storage)
            )

        branch.resize_task_id = None
        await session.commit()

    return {**succeeded, "errors": errors}


@app.task(name="simplyblock.vela.branch.resize.finalize")
def finalize_resize(
    job_results: list,
    branch_id: str,
    effective_parameters: dict,
    job_fields: list[list[str]],
) -> dict:
    """Chord callback: persist succeeded fields and clear resize_task_id."""
    return async_to_sync(_async_finalize_resize)(job_results, branch_id, effective_parameters, job_fields)


def dispatch_resize(branch_id: str, effective_parameters: dict) -> UUID:
    """Build and dispatch a chord of resize sub-tasks; return the chord result UUID."""
    jobs = []
    job_fields: list[list[str]] = []

    ep = effective_parameters
    if "milli_vcpu" in ep or "memory_bytes" in ep:
        jobs.append(resize_cpu_memory.s(branch_id, ep.get("milli_vcpu"), ep.get("memory_bytes")))
        job_fields.append([f for f in ("milli_vcpu", "memory_bytes") if f in ep])
    if "iops" in ep:
        jobs.append(resize_iops.s(branch_id, ep["iops"]))
        job_fields.append(["iops"])
    if "database_size" in ep:
        jobs.append(resize_database_pvc.s(branch_id, ep["database_size"]))
        job_fields.append(["database_size"])
    if "storage_size" in ep:
        jobs.append(resize_storage_pvc.s(branch_id, ep["storage_size"]))
        job_fields.append(["storage_size"])

    result = chord(jobs)(finalize_resize.s(branch_id=branch_id, effective_parameters=ep, job_fields=job_fields))
    return UUID(result.id)
