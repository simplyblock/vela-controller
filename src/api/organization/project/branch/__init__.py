import asyncio
import contextlib
import hashlib
import logging
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, TypedDict, cast
from urllib.parse import urlsplit, urlunsplit

import asyncpg
from asyncpg import exceptions as asyncpg_exceptions
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from keycloak.exceptions import KeycloakError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import ValidationError
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ....._util import DEFAULT_DB_NAME, DEFAULT_DB_USER, Identifier
from ....._util.crypto import encrypt_with_passphrase, generate_keys
from .....deployment import (
    DeploymentParameters,
    ResizeParameters,
    branch_api_domain,
    branch_domain,
    branch_rest_endpoint,
    branch_service_name,
    calculate_cpu_resources,
    delete_deployment,
    deploy_branch_environment,
    ensure_branch_storage_class,
    get_db_vmi_identity,
    kube_service,
    resize_deployment,
    update_branch_database_password,
    update_branch_volume_iops,
)
from .....deployment.kubernetes._util import core_v1_client
from .....deployment.kubernetes.kubevirt import KubevirtSubresourceAction, call_kubevirt_subresource
from .....deployment.kubernetes.volume_clone import (
    clone_branch_database_volume,
    restore_branch_database_volume_from_snapshot,
)
from .....deployment.settings import get_settings as get_deployment_settings
from .....exceptions import VelaError, VelaKubernetesError
from .....models.backups import BackupEntry
from .....models.branch import (
    ApiKeyDetails,
    Branch,
    BranchApiKeys,
    BranchCreate,
    BranchPasswordReset,
    BranchPgbouncerConfigStatus,
    BranchPgbouncerConfigUpdate,
    BranchPublic,
    BranchResizeService,
    BranchResizeStatusEntry,
    BranchServiceStatus,
    BranchSourceDeploymentParameters,
    BranchStatus,
    BranchStatusPublic,
    BranchUpdate,
    CapaResizeKey,
    DatabaseInformation,
    PgbouncerConfig,
    aggregate_resize_statuses,
)
from .....models.resources import BranchAllocationPublic, ResourceLimitsPublic, ResourceType
from ...._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...._util.backups import copy_branch_backup_schedules
from ...._util.resourcelimit import (
    check_available_resources_limits,
    create_or_update_branch_provisioning,
    delete_branch_provisioning,
    format_limit_violation_details,
    get_current_branch_allocations,
)
from ...._util.role import clone_user_role_assignment
from ....auth import security
from ....db import AsyncSessionLocal, SessionDep
from ....dependencies import BranchDep, OrganizationDep, ProjectDep, branch_lookup
from ....keycloak import realm_admin
from ....settings import get_settings as get_api_settings
from .auth import api as auth_api

api = APIRouter(tags=["branch"])

logger = logging.getLogger(__name__)


_TRANSITIONAL_BRANCH_STATUSES: set[BranchServiceStatus] = {
    BranchServiceStatus.CREATING,
    BranchServiceStatus.STARTING,
    BranchServiceStatus.STOPPING,
    BranchServiceStatus.RESTARTING,
    BranchServiceStatus.PAUSING,
    BranchServiceStatus.RESUMING,
    BranchServiceStatus.UPDATING,
    BranchServiceStatus.DELETING,
}
_PROTECTED_BRANCH_STATUSES: set[BranchServiceStatus] = {BranchServiceStatus.PAUSED}


def _parse_branch_status(value: BranchServiceStatus | str | None) -> BranchServiceStatus:
    if isinstance(value, BranchServiceStatus):
        return value
    if value:
        # Normalize to the canonical representation expected by the enum ("STARTING", "STOPPED", etc.).
        normalized_value = str(value).upper()
        member = BranchServiceStatus._value2member_map_.get(normalized_value)
        if member is not None:
            return cast("BranchServiceStatus", member)
        logger.warning("Encountered unknown branch status %s; defaulting to UNKNOWN", value)
    return BranchServiceStatus.UNKNOWN


def _apply_local_branch_status(branch: Branch, status: BranchServiceStatus) -> bool:
    state = inspect(branch)
    if state is not None and "status" in state.dict and _parse_branch_status(state.dict["status"]) == status:
        return False
    branch.status = status
    return True


async def _persist_branch_status(branch_id: Identifier, status: BranchServiceStatus) -> None:
    async with AsyncSessionLocal() as session:
        branch = await session.get(Branch, branch_id)
        if branch is None:
            logger.warning("Branch %s missing while updating status to %s", branch_id, status)
            return
        if _parse_branch_status(branch.status) == status:
            return
        branch.status = status
        await session.commit()


def _derive_branch_status_from_services(
    service_status: BranchStatus,
    *,
    storage_enabled: bool,
) -> BranchServiceStatus:
    statuses: list[BranchServiceStatus] = [
        service_status.database,
        service_status.meta,
        service_status.rest,
    ]
    if storage_enabled:
        statuses.append(service_status.storage)

    if all(status == BranchServiceStatus.ACTIVE_HEALTHY for status in statuses):
        return BranchServiceStatus.ACTIVE_HEALTHY
    if any(status == BranchServiceStatus.ERROR for status in statuses):
        return BranchServiceStatus.ERROR
    if all(status == BranchServiceStatus.STOPPED for status in statuses):
        return BranchServiceStatus.STOPPED
    if any(status == BranchServiceStatus.UNKNOWN for status in statuses):
        return BranchServiceStatus.UNKNOWN
    return BranchServiceStatus.ACTIVE_UNHEALTHY


def _should_update_branch_status(
    current: BranchServiceStatus,
    derived: BranchServiceStatus,
) -> bool:
    if current == derived:
        return False
    if current == BranchServiceStatus.STARTING and derived == BranchServiceStatus.STOPPED:
        logger.debug("Ignoring STARTING -> STOPPED transition detected by branch status monitor")
        return False
    if current in _PROTECTED_BRANCH_STATUSES and derived not in {
        BranchServiceStatus.ACTIVE_HEALTHY,
        BranchServiceStatus.ERROR,
    }:
        return False
    if (
        derived == BranchServiceStatus.STOPPED
        and current in _TRANSITIONAL_BRANCH_STATUSES
        and current != BranchServiceStatus.STOPPING
    ):
        return False
    if derived in {
        BranchServiceStatus.ACTIVE_HEALTHY,
        BranchServiceStatus.ACTIVE_UNHEALTHY,
        BranchServiceStatus.STOPPED,
        BranchServiceStatus.ERROR,
    }:
        return True
    if derived == BranchServiceStatus.UNKNOWN:
        return current not in _TRANSITIONAL_BRANCH_STATUSES and current not in _PROTECTED_BRANCH_STATUSES
    return True


async def refresh_branch_status(branch_id: Identifier) -> BranchServiceStatus:
    """
    Probe branch services, derive an overall lifecycle state, and persist it when appropriate.
    """
    async with AsyncSessionLocal() as session:
        branch = await session.get(Branch, branch_id)
        if branch is None:
            logger.warning("Branch %s not found while refreshing status", branch_id)
            return BranchServiceStatus.UNKNOWN

        current_status = _parse_branch_status(branch.status)
        try:
            namespace, _ = get_db_vmi_identity(branch.id)
            service_status = await _collect_branch_service_health(
                namespace,
                storage_enabled=branch.enable_file_storage,
            )
            derived_status = _derive_branch_status_from_services(
                service_status,
                storage_enabled=branch.enable_file_storage,
            )
        except Exception:
            logger.exception("Failed to refresh service status for branch %s", branch.id)
            derived_status = BranchServiceStatus.UNKNOWN

        if _should_update_branch_status(current_status, derived_status) and _apply_local_branch_status(
            branch,
            derived_status,
        ):
            await session.commit()
            return derived_status

        await session.rollback()
        return current_status


def _normalize_resize_statuses(branch: Branch) -> dict[str, BranchResizeStatusEntry]:
    statuses = branch.resize_statuses or {}
    if not statuses:
        return {}

    normalized: dict[str, BranchResizeStatusEntry] = {}
    for service, entry in statuses.items():
        if isinstance(entry, BranchResizeStatusEntry):
            normalized[service] = entry
            continue
        try:
            normalized[service] = BranchResizeStatusEntry.model_validate(entry)
        except ValidationError:
            logger.warning(
                "Skipping invalid resize status entry for branch %s service %s",
                branch.id,
                service,
            )
    return normalized


_DEFAULT_SERVICE_STATUS = BranchStatus(
    database=BranchServiceStatus.UNKNOWN,
    storage=BranchServiceStatus.UNKNOWN,
    meta=BranchServiceStatus.UNKNOWN,
    rest=BranchServiceStatus.UNKNOWN,
)


async def _branch_service_status(branch: Branch) -> BranchStatus:
    namespace, _ = get_db_vmi_identity(branch.id)
    try:
        return await _collect_branch_service_health(namespace, storage_enabled=branch.enable_file_storage)
    except Exception:  # pragma: no cover - defensive guard
        logging.exception("Failed to determine service health via socket probes")
        status = _DEFAULT_SERVICE_STATUS.model_copy(deep=True)
        if not branch.enable_file_storage:
            status.storage = BranchServiceStatus.STOPPED
        return status


async def _resolve_branch_status(
    branch: Branch,
    *,
    service_status: BranchStatus | None = None,
) -> BranchServiceStatus:
    current_status = _parse_branch_status(branch.status)
    if service_status is None:
        service_status = await _branch_service_status(branch)

    derived_status = _derive_branch_status_from_services(
        service_status,
        storage_enabled=branch.enable_file_storage,
    )
    if _should_update_branch_status(current_status, derived_status):
        await _persist_branch_status(branch.id, derived_status)
        branch.status = derived_status
        return derived_status
    return current_status


_SERVICE_PROBE_TIMEOUT_SECONDS = 2
_SNAPSHOT_TIMEOUT_SECONDS = float(600)
_SNAPSHOT_POLL_INTERVAL_SECONDS = float(2)
_PVC_TIMEOUT_SECONDS = float(600)
_PVC_POLL_INTERVAL_SECONDS = float(2)
_VOLUME_SNAPSHOT_CLASS = "simplyblock-csi-snapshotclass"
_SUPPORTED_DATABASE_IMAGE_TAG = "15.1.0.147"

_BRANCH_SERVICE_ENDPOINTS: dict[str, tuple[str, int]] = {
    "database": ("db", 5432),
    "pgbouncer": ("pgbouncer", 6432),
    "storage": ("storage", 5000),
    "meta": ("meta", 8080),
    "rest": ("rest", 3000),
    "pgexporter": ("pgexporter", 9187),
}

_PGBOUNCER_ADMIN_USER = "pgbouncer_admin"
_PGBOUNCER_ADMIN_DATABASE = "pgbouncer"
_PGBOUNCER_SERVICE_PORT = 6432
_PGBOUNCER_CONFIG_TEMPLATE_ERROR = "PgBouncer configuration template missing required entries."
_PGBOUNCER_CONFIG_UPDATE_ERROR = "Failed to update PgBouncer configuration."


def generate_pgbouncer_password(length: int = 32) -> str:
    if length <= 0:
        raise ValueError("PgBouncer password length must be positive.")
    password = ""
    # secrets.token_urlsafe returns roughly 4/3 * n characters, so loop until we have enough.
    while len(password) < length:
        password += secrets.token_urlsafe(length)
    return password[:length]


async def _copy_pgbouncer_config_from_source(source: Branch) -> PgbouncerConfig:
    config = await source.awaitable_attrs.pgbouncer_config
    if config is None:
        return _default_pgbouncer_config()
    return PgbouncerConfig(
        default_pool_size=config.default_pool_size,
        max_client_conn=config.max_client_conn,
        query_wait_timeout=config.query_wait_timeout,
        reserve_pool_size=config.reserve_pool_size,
        server_idle_timeout=config.server_idle_timeout,
        server_lifetime=config.server_lifetime,
    )


def _default_pgbouncer_config() -> PgbouncerConfig:
    return PgbouncerConfig(
        default_pool_size=PgbouncerConfig.DEFAULT_POOL_SIZE,
        max_client_conn=PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN,
        query_wait_timeout=PgbouncerConfig.DEFAULT_QUERY_WAIT_TIMEOUT,
        reserve_pool_size=PgbouncerConfig.DEFAULT_RESERVE_POOL_SIZE,
        server_idle_timeout=PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT,
        server_lifetime=PgbouncerConfig.DEFAULT_SERVER_LIFETIME,
    )


class PgbouncerConfigSnapshot(TypedDict):
    default_pool_size: int
    max_client_conn: int | None
    query_wait_timeout: int | None
    reserve_pool_size: int | None
    server_idle_timeout: int | None
    server_lifetime: int | None


class RestoreSnapshotContext(TypedDict):
    namespace: str
    name: str
    content_name: str | None


def snapshot_pgbouncer_config(config: PgbouncerConfig | None) -> PgbouncerConfigSnapshot:
    if config is None:
        config = _default_pgbouncer_config()
    max_client_conn = (
        config.max_client_conn if config.max_client_conn is not None else PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN
    )
    server_idle_timeout = (
        config.server_idle_timeout
        if config.server_idle_timeout is not None
        else PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT
    )
    server_lifetime = (
        config.server_lifetime if config.server_lifetime is not None else PgbouncerConfig.DEFAULT_SERVER_LIFETIME
    )
    query_wait_timeout = (
        config.query_wait_timeout
        if config.query_wait_timeout is not None
        else PgbouncerConfig.DEFAULT_QUERY_WAIT_TIMEOUT
    )
    reserve_pool_size = (
        config.reserve_pool_size if config.reserve_pool_size is not None else PgbouncerConfig.DEFAULT_RESERVE_POOL_SIZE
    )
    return PgbouncerConfigSnapshot(
        default_pool_size=config.default_pool_size,
        max_client_conn=max_client_conn,
        query_wait_timeout=query_wait_timeout,
        reserve_pool_size=reserve_pool_size,
        server_idle_timeout=server_idle_timeout,
        server_lifetime=server_lifetime,
    )


def _resolve_pgbouncer_setting(value: int | None, default: int | None, *, setting: str) -> int:
    if value is not None:
        return value
    if default is None:
        raise ValueError(f"PgBouncer default {setting} is not configured")
    return default


def pgbouncer_snapshot_to_mapping(snapshot: PgbouncerConfigSnapshot) -> dict[str, int]:
    resolved_settings = {
        "max_client_conn": _resolve_pgbouncer_setting(
            snapshot["max_client_conn"],
            PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN,
            setting="max_client_conn",
        ),
        "query_wait_timeout": _resolve_pgbouncer_setting(
            snapshot["query_wait_timeout"],
            PgbouncerConfig.DEFAULT_QUERY_WAIT_TIMEOUT,
            setting="query_wait_timeout",
        ),
        "reserve_pool_size": _resolve_pgbouncer_setting(
            snapshot["reserve_pool_size"],
            PgbouncerConfig.DEFAULT_RESERVE_POOL_SIZE,
            setting="reserve_pool_size",
        ),
        "server_idle_timeout": _resolve_pgbouncer_setting(
            snapshot["server_idle_timeout"],
            PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT,
            setting="server_idle_timeout",
        ),
        "server_lifetime": _resolve_pgbouncer_setting(
            snapshot["server_lifetime"],
            PgbouncerConfig.DEFAULT_SERVER_LIFETIME,
            setting="server_lifetime",
        ),
    }

    return {"default_pool_size": snapshot["default_pool_size"], **resolved_settings}


class _DeploymentResourceValues(TypedDict):
    database_size: int | None
    storage_size: int | None
    milli_vcpu: int | None
    memory_bytes: int | None
    iops: int | None


def _normalize_database_image_tag(image_tag: str, branch_id: Identifier) -> str:
    if image_tag != _SUPPORTED_DATABASE_IMAGE_TAG:  # pragma: no cover - defensive guard against unsupported images
        logger.warning(
            "Source branch %s has unexpected database image tag %s; defaulting to supported image",
            branch_id,
            image_tag,
        )
        return _SUPPORTED_DATABASE_IMAGE_TAG
    return image_tag


def _base_deployment_resources(
    source: Branch,
    source_limits: BranchAllocationPublic | None,
) -> _DeploymentResourceValues:
    def _value_from_limits(attribute: str, fallback: Any) -> Any:
        if source_limits is None:
            return fallback
        limit_value = getattr(source_limits, attribute)
        return fallback if limit_value is None else limit_value

    return {
        "database_size": _value_from_limits("database_size", source.database_size),
        "storage_size": _value_from_limits("storage_size", source.storage_size),
        "milli_vcpu": _value_from_limits("milli_vcpu", source.milli_vcpu),
        "memory_bytes": _value_from_limits("ram", source.memory),
        "iops": _value_from_limits("iops", source.iops),
    }


def _apply_overrides_to_resources(
    base_values: _DeploymentResourceValues,
    *,
    overrides: BranchSourceDeploymentParameters | None,
    enable_file_storage: bool,
) -> tuple[_DeploymentResourceValues, bool]:
    if overrides is None:
        return base_values, enable_file_storage

    def _with_minimum(
        override_value: int | None,
        current_value: int | None,
        *,
        error_detail: str,
    ) -> int | None:
        if override_value is None:
            return current_value
        if current_value is not None and override_value < current_value:
            raise HTTPException(status_code=422, detail=error_detail)
        return override_value

    updated = dict(base_values)
    updated["database_size"] = _with_minimum(
        overrides.database_size,
        base_values["database_size"],
        error_detail="database_size override must be greater than or equal to the source branch allocation",
    )
    updated["storage_size"] = _with_minimum(
        overrides.storage_size,
        base_values["storage_size"],
        error_detail="storage_size override must be greater than or equal to the source branch allocation",
    )
    if overrides.milli_vcpu is not None:
        updated["milli_vcpu"] = overrides.milli_vcpu
    if overrides.memory_bytes is not None:
        updated["memory_bytes"] = overrides.memory_bytes
    if overrides.iops is not None:
        updated["iops"] = overrides.iops
    if overrides.enable_file_storage is not None:
        enable_file_storage = overrides.enable_file_storage

    return cast("_DeploymentResourceValues", updated), enable_file_storage


def _validate_deployment_requirements(
    resources: _DeploymentResourceValues,
    *,
    enable_file_storage: bool,
) -> None:
    if resources["database_size"] is None:
        raise HTTPException(
            status_code=422,
            detail="database_size is required when cloning from a source branch",
        )
    if resources["milli_vcpu"] is None:
        raise HTTPException(
            status_code=422,
            detail="milli_vcpu is required when cloning from a source branch",
        )
    if resources["memory_bytes"] is None:
        raise HTTPException(
            status_code=422,
            detail="memory_bytes is required when cloning from a source branch",
        )
    if resources["iops"] is None:
        raise HTTPException(
            status_code=422,
            detail="iops is required when cloning from a source branch",
        )
    if enable_file_storage and resources["storage_size"] is None:
        raise HTTPException(
            status_code=422,
            detail="storage_size must be provided when file storage is enabled for the new branch",
        )


def _deployment_parameters_from_source(
    source: Branch,
    *,
    source_limits: BranchAllocationPublic | None = None,
    overrides: BranchSourceDeploymentParameters | None = None,
) -> DeploymentParameters:
    image_tag = _normalize_database_image_tag(source.database_image_tag, source.id)
    resource_values = _base_deployment_resources(source, source_limits)
    enable_file_storage = source.enable_file_storage

    resource_values, enable_file_storage = _apply_overrides_to_resources(
        resource_values,
        overrides=overrides,
        enable_file_storage=enable_file_storage,
    )

    _validate_deployment_requirements(resource_values, enable_file_storage=enable_file_storage)
    database_size = cast("int", resource_values["database_size"])
    milli_vcpu = cast("int", resource_values["milli_vcpu"])
    memory_bytes = cast("int", resource_values["memory_bytes"])
    iops = cast("int", resource_values["iops"])
    storage_size = resource_values["storage_size"] if enable_file_storage else None

    return DeploymentParameters(
        database_password=source.database_password,
        database_size=database_size,
        storage_size=storage_size,
        milli_vcpu=milli_vcpu,
        memory_bytes=memory_bytes,
        iops=iops,
        database_image_tag=cast("Literal['15.1.0.147']", image_tag),
        enable_file_storage=enable_file_storage,
    )


def _resource_limits_from_deployment(parameters: DeploymentParameters) -> ResourceLimitsPublic:
    return ResourceLimitsPublic(
        milli_vcpu=parameters.milli_vcpu,
        ram=parameters.memory_bytes,
        iops=parameters.iops,
        database_size=parameters.database_size,
        storage_size=parameters.storage_size,
    )


async def _build_branch_entity(
    *,
    project: ProjectDep,
    parameters: BranchCreate,
    source: Branch | None,
    copy_config: bool,
    clone_parameters: DeploymentParameters | None,
) -> Branch:
    env_type = parameters.env_type if parameters.env_type is not None else ""
    if source is not None:
        if clone_parameters is None:
            raise AssertionError("clone_parameters required when cloning from a source branch")

        entity = Branch(
            name=parameters.name,
            project_id=project.id,
            parent_id=source.id,
            database=DEFAULT_DB_NAME,
            database_user=DEFAULT_DB_USER,
            database_size=clone_parameters.database_size,
            storage_size=clone_parameters.storage_size if clone_parameters.enable_file_storage else None,
            milli_vcpu=clone_parameters.milli_vcpu,
            memory=clone_parameters.memory_bytes,
            iops=clone_parameters.iops,
            database_image_tag=clone_parameters.database_image_tag,
            env_type=env_type,
            enable_file_storage=clone_parameters.enable_file_storage,
            status=BranchServiceStatus.CREATING,
        )
        entity.database_password = source.database_password
        entity.pgbouncer_config = (
            await _copy_pgbouncer_config_from_source(source) if copy_config else _default_pgbouncer_config()
        )
        return entity

    deployment_params = cast("DeploymentParameters", parameters.deployment)
    entity = Branch(
        name=parameters.name,
        project_id=project.id,
        parent=None,
        database=DEFAULT_DB_NAME,
        database_user=DEFAULT_DB_USER,
        database_size=deployment_params.database_size,
        storage_size=deployment_params.storage_size if deployment_params.enable_file_storage else None,
        milli_vcpu=deployment_params.milli_vcpu,
        memory=deployment_params.memory_bytes,
        iops=deployment_params.iops,
        database_image_tag=deployment_params.database_image_tag,
        env_type=env_type,
        enable_file_storage=deployment_params.enable_file_storage,
        status=BranchServiceStatus.CREATING,
    )
    entity.database_password = deployment_params.database_password
    entity.pgbouncer_config = _default_pgbouncer_config()
    return entity


async def _probe_service_socket(host: str, port: int, *, label: str) -> BranchServiceStatus:
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host=host, port=port),
            timeout=_SERVICE_PROBE_TIMEOUT_SECONDS,
        )
    except (TimeoutError, OSError):
        logger.debug("Service %s unavailable at %s:%s", label, host, port)
        return BranchServiceStatus.STOPPED
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Unexpected error probing service %s", label)
        return BranchServiceStatus.UNKNOWN

    writer.close()
    try:
        await writer.wait_closed()
    except OSError:  # pragma: no cover - best effort socket cleanup
        logger.debug("Failed to close probe socket for %s", label, exc_info=True)
    return BranchServiceStatus.ACTIVE_HEALTHY


async def _collect_branch_service_health(namespace: str, *, storage_enabled: bool) -> BranchStatus:
    endpoints = {
        label: (branch_service_name(namespace, component), port)
        for label, (component, port) in _BRANCH_SERVICE_ENDPOINTS.items()
        if storage_enabled or label != "storage"
    }

    probes = {
        label: asyncio.create_task(
            _probe_service_socket(
                host=f"{service_name}.{namespace}.svc.cluster.local",
                port=port,
                label=label,
            )
        )
        for label, (service_name, port) in endpoints.items()
    }

    results: dict[str, BranchServiceStatus] = {}
    for label, task in probes.items():
        try:
            results[label] = await task
        except Exception:  # pragma: no cover - unexpected failures
            logger.exception("Service health probe failed for %s", label)
            results[label] = BranchServiceStatus.UNKNOWN

    return BranchStatus(
        database=results["database"],
        storage=results.get(
            "storage",
            BranchServiceStatus.STOPPED if not storage_enabled else BranchServiceStatus.UNKNOWN,
        ),
        meta=results["meta"],
        rest=results["rest"],
    )


_PARAMETER_TO_SERVICE: dict[CapaResizeKey, BranchResizeService] = {
    "database_size": "database_disk_resize",
    "storage_size": "storage_api_disk_resize",
    "milli_vcpu": "database_cpu_resize",
    "memory_bytes": "database_memory_resize",
    "iops": "database_iops_resize",
}


def _track_resize_change(
    *,
    parameter_key: CapaResizeKey,
    new_value: int | None,
    current_value: int | None,
    statuses: dict[str, dict[str, Any]],
    effective: dict[CapaResizeKey, int],
    timestamp: str,
) -> None:
    service_key = _PARAMETER_TO_SERVICE[parameter_key]
    if new_value is None:
        return
    if new_value != current_value:
        effective[parameter_key] = new_value
        entry: dict[str, Any] = {"status": "PENDING", "timestamp": timestamp, "requested_at": timestamp}
        statuses[service_key] = entry
    elif statuses.get(service_key, {}).get("status") == "PENDING":
        statuses.pop(service_key, None)


# TODO: send an alert if function fails.
async def _sync_branch_cpu_resources(
    branch_id: Identifier,
    *,
    desired_milli_vcpu: int,
    attempts: int = 10,
    delay_seconds: float = 2.0,
) -> None:
    """
    Ensure the virt-launcher pod backing the branch VM reflects the desired CPU settings.
    Retries while the pod is recreating (e.g. immediately after a start request).
    """
    namespace, vm_name = get_db_vmi_identity(branch_id)

    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            cpu_limit, cpu_request = calculate_cpu_resources(desired_milli_vcpu)
            await kube_service.resize_vm_compute_cpu(
                namespace,
                vm_name,
                cpu_request=cpu_request,
                cpu_limit=cpu_limit,
            )
            return
        except VelaKubernetesError as exc:
            last_error = exc
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(delay_seconds)

    if last_error is not None:
        raise last_error


async def _apply_resize_operations(
    session: SessionDep,
    branch: Branch,
    effective_parameters: dict[CapaResizeKey, int],
) -> None:
    resize_params = ResizeParameters(**{str(key): value for key, value in effective_parameters.items()})
    resize_deployment(branch.id, resize_params)

    if "iops" in effective_parameters:
        new_iops = effective_parameters["iops"]
        await update_branch_volume_iops(branch.id, new_iops)
        branch.iops = new_iops
        await create_or_update_branch_provisioning(
            session,
            branch,
            ResourceLimitsPublic(iops=new_iops),
            commit=False,
        )

    milli_vcpu = effective_parameters.get("milli_vcpu")
    if milli_vcpu is not None:
        await _sync_branch_cpu_resources(
            branch.id,
            desired_milli_vcpu=milli_vcpu,
        )
        branch.milli_vcpu = milli_vcpu
        await create_or_update_branch_provisioning(
            session,
            branch,
            ResourceLimitsPublic(milli_vcpu=milli_vcpu),
            commit=False,
        )


async def _deploy_branch_environment_task(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    credential: str,
    branch_id: Identifier,
    branch_slug: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    pgbouncer_config: PgbouncerConfigSnapshot,
) -> None:
    await _persist_branch_status(branch_id, BranchServiceStatus.CREATING)
    try:
        await deploy_branch_environment(
            organization_id=organization_id,
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            credential=credential,
            parameters=parameters,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
            pgbouncer_admin_password=pgbouncer_admin_password,
            pgbouncer_config=pgbouncer_snapshot_to_mapping(pgbouncer_config),
        )
    except VelaError:
        await _persist_branch_status(branch_id, BranchServiceStatus.ERROR)
        logging.exception(
            "Branch deployment failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )
        return
    await _persist_branch_status(branch_id, BranchServiceStatus.STARTING)


async def _clone_branch_environment_task(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    credential: str,
    branch_id: Identifier,
    branch_slug: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    source_branch_id: Identifier,
    copy_data: bool,
    pgbouncer_config: PgbouncerConfigSnapshot,
) -> None:
    await _persist_branch_status(branch_id, BranchServiceStatus.CREATING)
    storage_class_name: str | None = None
    if copy_data:
        try:
            storage_class_name = await ensure_branch_storage_class(branch_id, iops=parameters.iops)
            await clone_branch_database_volume(
                source_branch_id=source_branch_id,
                target_branch_id=branch_id,
                snapshot_class=_VOLUME_SNAPSHOT_CLASS,
                storage_class_name=storage_class_name,
                snapshot_timeout_seconds=_SNAPSHOT_TIMEOUT_SECONDS,
                snapshot_poll_interval_seconds=_SNAPSHOT_POLL_INTERVAL_SECONDS,
                pvc_timeout_seconds=_PVC_TIMEOUT_SECONDS,
                pvc_poll_interval_seconds=_PVC_POLL_INTERVAL_SECONDS,
            )
        except VelaError:
            await _persist_branch_status(branch_id, BranchServiceStatus.ERROR)
            logging.exception(
                "Branch data clone failed for project_id=%s branch_id=%s branch_slug=%s",
                project_id,
                branch_id,
                branch_slug,
            )
            return

    try:
        await deploy_branch_environment(
            organization_id=organization_id,
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            credential=credential,
            parameters=parameters,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
            pgbouncer_admin_password=pgbouncer_admin_password,
            use_existing_pvc=copy_data,
            pgbouncer_config=pgbouncer_snapshot_to_mapping(pgbouncer_config),
        )
    except VelaError:
        await _persist_branch_status(branch_id, BranchServiceStatus.ERROR)
        logging.exception(
            "Branch deployment (clone) failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )
        return
    await _persist_branch_status(branch_id, BranchServiceStatus.STARTING)


async def _restore_branch_environment_task(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    credential: str,
    branch_id: Identifier,
    branch_slug: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    source_branch_id: Identifier,
    snapshot_namespace: str,
    snapshot_name: str,
    snapshot_content_name: str | None,
    pgbouncer_config: PgbouncerConfigSnapshot,
) -> None:
    await _persist_branch_status(branch_id, BranchServiceStatus.CREATING)
    storage_class_name: str | None = None
    try:
        storage_class_name = await ensure_branch_storage_class(branch_id, iops=parameters.iops)
        await restore_branch_database_volume_from_snapshot(
            source_branch_id=source_branch_id,
            target_branch_id=branch_id,
            snapshot_namespace=snapshot_namespace,
            snapshot_name=snapshot_name,
            snapshot_content_name=snapshot_content_name,
            snapshot_class=_VOLUME_SNAPSHOT_CLASS,
            storage_class_name=storage_class_name,
            snapshot_timeout_seconds=_SNAPSHOT_TIMEOUT_SECONDS,
            snapshot_poll_interval_seconds=_SNAPSHOT_POLL_INTERVAL_SECONDS,
            pvc_timeout_seconds=_PVC_TIMEOUT_SECONDS,
            pvc_poll_interval_seconds=_PVC_POLL_INTERVAL_SECONDS,
        )
    except VelaError:
        await _persist_branch_status(branch_id, BranchServiceStatus.ERROR)
        logging.exception(
            "Branch restore failed for project_id=%s branch_id=%s branch_slug=%s using snapshot %s/%s",
            project_id,
            branch_id,
            branch_slug,
            snapshot_namespace,
            snapshot_name,
        )
        return

    try:
        await deploy_branch_environment(
            organization_id=organization_id,
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
            credential=credential,
            parameters=parameters,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
            pgbouncer_admin_password=pgbouncer_admin_password,
            use_existing_pvc=True,
            pgbouncer_config=pgbouncer_snapshot_to_mapping(pgbouncer_config),
        )
    except VelaError:
        await _persist_branch_status(branch_id, BranchServiceStatus.ERROR)
        logging.exception(
            "Branch deployment (restore) failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )
        return
    await _persist_branch_status(branch_id, BranchServiceStatus.STARTING)


def _resolve_db_host(branch: Branch) -> str | None:
    host = branch.endpoint_domain or branch_domain(branch.id)
    return host or get_deployment_settings().deployment_host


def _build_connection_string(user: str, database: str, port: int) -> str:
    return "postgresql://{user}@{host}:{port}/{database}".format(  # noqa: UP032
        user=user,
        host="db",
        port=port,
        database=database,
    )


def _ensure_service_port(url: str, port: int) -> str:
    if port == 443:
        return url
    split = urlsplit(url)
    if not split.netloc or ":" in split.netloc:
        return url
    netloc = f"{split.netloc}:{port}"
    return urlunsplit((split.scheme, netloc, split.path, split.query, split.fragment))


def _service_endpoint_url(rest_endpoint: str | None, api_domain: str | None, db_host: str | None) -> str:
    if rest_endpoint:
        candidate = rest_endpoint.removesuffix("/rest")
    elif api_domain:
        candidate = f"https://{api_domain}"
    else:
        candidate = f"https://{db_host or ''}"
    return _ensure_service_port(candidate, get_deployment_settings().deployment_service_port)


async def _public(branch: Branch) -> BranchPublic:
    project = await branch.awaitable_attrs.project

    db_host = _resolve_db_host(branch) or ""
    port = 5432

    # pg-meta and pg are in the same network. So password is not required in connection string.
    connection_string = _build_connection_string(branch.database_user, "postgres", port)

    rest_endpoint = branch_rest_endpoint(branch.id)
    api_domain = branch_api_domain(branch.id)
    # Fall back to using the same host as the database when dedicated domains are unavailable.
    service_endpoint = _service_endpoint_url(rest_endpoint, api_domain, db_host)

    max_resources = branch.provisioned_resources()

    database_info = DatabaseInformation(
        host=db_host,
        port=port,
        username=branch.database_user,
        name=branch.database,
        encrypted_connection_string=encrypt_with_passphrase(connection_string, get_api_settings().pgmeta_crypto_key),
        service_endpoint_uri=service_endpoint,
        monitoring_endpoint_uri=f"{service_endpoint}/grafana/d/{branch.id}/metrics",
        version=branch.database_image_tag,
        has_replicas=False,
    )

    used_resources = branch.resource_usage_snapshot()
    branch_status = _parse_branch_status(branch.status)

    api_keys = BranchApiKeys(anon=branch.anon_key, service_role=branch.service_key)

    return BranchPublic(
        id=branch.id,
        name=branch.name,
        project_id=branch.project_id,
        organization_id=project.organization_id,
        database=database_info,
        env_type=branch.env_type,
        max_resources=max_resources,
        assigned_labels=[],
        used_resources=used_resources,
        api_keys=api_keys,
        status=branch_status,
        pitr_enabled=False,
        created_at=branch.created_datetime,
        created_by="system",  # TODO: update it when user management is in place
        updated_at=None,
        updated_by=None,
    )


@api.get(
    "/",
    name="organizations:projects:branch:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_branches(
    session: SessionDep,
    _organization: OrganizationDep,
    project: ProjectDep,
) -> Sequence[BranchPublic]:
    await session.refresh(project, ["branches"])
    branches = await project.awaitable_attrs.branches
    return [await _public(branch) for branch in branches]


_links = {
    "detail": {
        "operationId": "organizations:projects:branch:detail",
        "parameters": {
            "project_id": "$response.header.Location#regex:/projects/(.+)/",
            "branch_id": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "update": {
        "operationId": "organizations:projects:branch:update",
        "parameters": {
            "project_id": "$response.header.Location#regex:/projects/(.+)/",
            "branch_id": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
    "delete": {
        "operationId": "organizations:projects:branch:delete",
        "parameters": {
            "project_id": "$response.header.Location#regex:/projects/(.+)/",
            "branch_id": "$response.header.Location#regex:/branches/(.+)/",
        },
    },
}


def _validate_branch_create_request(parameters: BranchCreate) -> None:
    if parameters.source is None and parameters.deployment is None and parameters.restore is None:
        raise HTTPException(400, "Either source, deployment, or restore parameters must be provided")


async def _resolve_source_branch(
    session: SessionDep,
    project: ProjectDep,
    _organization: OrganizationDep,
    parameters: BranchCreate,
) -> tuple[Branch | None, BackupEntry | None]:
    if parameters.source is not None:
        branch = await branch_lookup(session, project, parameters.source.branch_id)
        return branch, None

    if parameters.restore is not None:
        backup_id = parameters.restore.backup_id
        result = await session.execute(select(BackupEntry).where(BackupEntry.id == backup_id))
        backup = result.scalar_one_or_none()
        if backup is None:
            raise HTTPException(404, f"Backup {backup_id} not found")
        try:
            branch = await branch_lookup(session, project, backup.branch_id)
        except HTTPException as exc:
            if exc.status_code == 404:
                raise HTTPException(404, f"Backup {backup_id} not found") from exc
            raise
        if not backup.snapshot_name or not backup.snapshot_namespace:
            raise HTTPException(400, "Selected backup does not include complete snapshot metadata")
        return branch, backup

    return None, None


def _ensure_branch_resource_limits(
    exceeded_limits: Sequence[ResourceType],
    resource_requests: ResourceLimitsPublic,
    remaining_limits: ResourceLimitsPublic,
) -> None:
    if exceeded_limits:
        violation_details = format_limit_violation_details(exceeded_limits, resource_requests, remaining_limits)
        raise HTTPException(422, f"New branch will exceed limit(s): {violation_details}")


def _build_target_allocations(branch: Branch, updates: dict[CapaResizeKey, int]) -> ResourceLimitsPublic:
    storage_override = updates.get("storage_size")
    return ResourceLimitsPublic(
        milli_vcpu=updates.get("milli_vcpu", branch.milli_vcpu),
        ram=updates.get("memory_bytes", branch.memory),
        iops=updates.get("iops", branch.iops),
        database_size=updates.get("database_size", branch.database_size),
        storage_size=storage_override if storage_override is not None else branch.storage_size,
    )


async def _ensure_resize_resource_limits(
    session: SessionDep,
    branch: Branch,
    updates: dict[CapaResizeKey, int],
) -> None:
    if not updates:
        return

    project = await branch.awaitable_attrs.project
    target_allocations = _build_target_allocations(branch, updates)
    exceeded_limits, remaining_limits = await check_available_resources_limits(
        session,
        project.organization_id,
        project.id,
        target_allocations,
        exclude_branch_ids=[branch.id],
    )
    _ensure_branch_resource_limits(exceeded_limits, target_allocations, remaining_limits)


async def _post_commit_branch_setup(
    session: SessionDep,
    source_branch_id: Identifier | None,
    *,
    copy_config: bool,
    entity: Branch,
) -> PgbouncerConfigSnapshot:
    await session.refresh(entity)
    if source_branch_id is not None and copy_config:
        await copy_branch_backup_schedules(session, source_branch_id, entity)
        await clone_user_role_assignment(session, source_branch_id, entity)
    return snapshot_pgbouncer_config(await entity.awaitable_attrs.pgbouncer_config)


def _schedule_branch_environment_tasks(
    *,
    deployment_parameters: DeploymentParameters | None,
    organization_id: Identifier,
    project_id: Identifier,
    credential: str,
    branch: Branch,
    clone_parameters: DeploymentParameters | None,
    source_id: Identifier | None,
    copy_data: bool,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    pgbouncer_config: PgbouncerConfigSnapshot,
    restore_snapshot: RestoreSnapshotContext | None,
) -> None:
    if deployment_parameters is not None:
        asyncio.create_task(
            _deploy_branch_environment_task(
                organization_id=organization_id,
                project_id=project_id,
                credential=credential,
                branch_id=branch.id,
                branch_slug=branch.name,
                parameters=deployment_parameters,
                jwt_secret=jwt_secret,
                anon_key=anon_key,
                service_key=service_key,
                pgbouncer_admin_password=pgbouncer_admin_password,
                pgbouncer_config=pgbouncer_config,
            )
        )
        return
    if restore_snapshot is not None and source_id is not None and clone_parameters is not None:
        asyncio.create_task(
            _restore_branch_environment_task(
                organization_id=organization_id,
                project_id=project_id,
                credential=credential,
                branch_id=branch.id,
                branch_slug=branch.name,
                parameters=clone_parameters,
                jwt_secret=jwt_secret,
                anon_key=anon_key,
                service_key=service_key,
                pgbouncer_admin_password=pgbouncer_admin_password,
                source_branch_id=source_id,
                snapshot_namespace=restore_snapshot["namespace"],
                snapshot_name=restore_snapshot["name"],
                snapshot_content_name=restore_snapshot["content_name"],
                pgbouncer_config=pgbouncer_config,
            )
        )
        return
    if source_id is not None and clone_parameters is not None:
        asyncio.create_task(
            _clone_branch_environment_task(
                organization_id=organization_id,
                project_id=project_id,
                credential=credential,
                branch_id=branch.id,
                branch_slug=branch.name,
                parameters=clone_parameters,
                jwt_secret=jwt_secret,
                anon_key=anon_key,
                service_key=service_key,
                pgbouncer_admin_password=pgbouncer_admin_password,
                source_branch_id=source_id,
                copy_data=copy_data,
                pgbouncer_config=pgbouncer_config,
            )
        )


@api.post(
    "/",
    name="organizations:projects:branch:create",
    status_code=201,
    response_model=BranchPublic | None,
    responses={
        201: {
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": _links,
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def create(
    session: SessionDep,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    organization: OrganizationDep,
    project: ProjectDep,
    parameters: BranchCreate,
    response: Literal["empty", "full"] = "empty",
) -> JSONResponse:
    _validate_branch_create_request(parameters)
    source, backup_entry = await _resolve_source_branch(session, project, organization, parameters)
    restore_snapshot_context: RestoreSnapshotContext | None = None
    if backup_entry is not None:
        restore_snapshot_context = RestoreSnapshotContext(
            namespace=cast("str", backup_entry.snapshot_namespace),
            name=cast("str", backup_entry.snapshot_name),
            content_name=backup_entry.snapshot_content_name,
        )
    source_id: Identifier | None = getattr(source, "id", None)
    clone_parameters: DeploymentParameters | None = None
    if source is not None:
        source_overrides = None
        if parameters.source is not None:
            source_overrides = parameters.source.deployment_parameters
        elif parameters.restore is not None:
            source_overrides = parameters.restore.deployment_parameters
        source_limits = await get_current_branch_allocations(session, source)
        clone_parameters = _deployment_parameters_from_source(
            source,
            source_limits=source_limits,
            overrides=source_overrides,
        )
        resource_requests = _resource_limits_from_deployment(clone_parameters)
    else:
        if parameters.deployment is None:
            raise HTTPException(status_code=400, detail="Either source or deployment parameters must be provided")
        resource_requests = _resource_limits_from_deployment(parameters.deployment)
    exceeded_limits, remaining_limits = await check_available_resources_limits(
        session, project.organization_id, project.id, resource_requests
    )
    _ensure_branch_resource_limits(exceeded_limits, resource_requests, remaining_limits)

    if parameters.restore is not None:
        copy_config = parameters.restore.config_copy
    else:
        copy_config = bool(parameters.source and parameters.source.config_copy)
    copy_data = bool(parameters.source and parameters.source.data_copy)

    entity = await _build_branch_entity(
        project=project,
        parameters=parameters,
        source=source,
        copy_config=copy_config,
        clone_parameters=clone_parameters,
    )
    jwt_secret, anon_key, service_key = generate_keys(str(entity.id))
    entity.jwt_secret = jwt_secret
    entity.anon_key = anon_key
    entity.service_key = service_key
    pgbouncer_admin_password = generate_pgbouncer_password()
    entity.pgbouncer_password = pgbouncer_admin_password
    session.add(entity)
    try:
        await realm_admin("master").a_create_realm(
            {
                "realm": str(entity.id),
                "eventsEnabled": True,
                "adminEventsEnabled": True,
            }
        )
        await realm_admin(str(entity.id)).a_create_client({"clientId": "application-client"})
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        error = str(exc)
        if "asyncpg.exceptions.UniqueViolationError" in error and "unique_branch_name_per_project" in error:
            raise HTTPException(409, f"Project already has branch named {parameters.name}") from exc
        raise
    except KeycloakError:
        await session.rollback()
        logging.exception("Failed to connect to keycloak")
        raise
    pgbouncer_config_snapshot = await _post_commit_branch_setup(
        session,
        source_id,
        copy_config=copy_config,
        entity=entity,
    )

    # Configure allocations
    await create_or_update_branch_provisioning(session, entity, resource_requests)

    entity_url = url_path_for(
        request,
        "organizations:projects:branch:detail",
        organization_id=await organization.awaitable_attrs.id,
        project_id=await project.awaitable_attrs.id,
        branch_id=entity.id,
    )
    restore_snapshot: RestoreSnapshotContext | None = restore_snapshot_context

    _schedule_branch_environment_tasks(
        deployment_parameters=parameters.deployment,
        organization_id=organization.id,
        project_id=project.id,
        credential=credentials.credentials,
        branch=entity,
        clone_parameters=clone_parameters,
        source_id=source_id,
        copy_data=copy_data,
        jwt_secret=entity.jwt_secret,
        anon_key=entity.anon_key,
        service_key=entity.service_key,
        pgbouncer_admin_password=pgbouncer_admin_password,
        pgbouncer_config=pgbouncer_config_snapshot,
        restore_snapshot=restore_snapshot,
    )

    payload = (await _public(entity)).model_dump(mode="json") if response == "full" else None

    return JSONResponse(
        content=payload,
        status_code=201,
        headers={"Location": entity_url},
    )


instance_api = APIRouter(prefix="/{branch_id}", tags=["branch"])


@instance_api.get(
    "/",
    name="organizations:projects:branch:detail",
    response_model=BranchPublic,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def detail(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> BranchPublic:
    return await _public(branch)


@instance_api.get(
    "/status",
    name="organizations:projects:branch:status",
    response_model=BranchStatusPublic,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def status(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> BranchStatusPublic:
    normalized_resize_statuses = _normalize_resize_statuses(branch)
    service_status = await _branch_service_status(branch)
    return BranchStatusPublic(
        resize_status=branch.resize_status,
        resize_statuses=normalized_resize_statuses,
        service_status=service_status,
    )


@instance_api.put(
    "/",
    name="organizations:projects:branch:update",
    status_code=204,
    responses={
        204: {
            "content": None,
            "headers": {
                "Location": {
                    "description": "URL of the created item",
                    "schema": {"type": "string"},
                },
            },
            "links": _links,
        },
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        409: Conflict,
    },
)
async def update(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
    parameters: BranchUpdate,
):
    update_values = parameters.model_dump(exclude_unset=True, exclude_none=True)
    if "name" in update_values:
        branch.name = update_values["name"]
        try:
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            error = str(exc)
            if "asyncpg.exceptions.UniqueViolationError" in error and "unique_branch_name_per_project" in error:
                raise HTTPException(409, f"Project already has branch named {parameters.name}") from exc
            raise

    return Response(status_code=204)


@instance_api.delete(
    "/",
    name="organizations:projects:branch:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
):
    await delete_deployment(branch.id)
    try:
        await realm_admin("master").a_delete_realm(str(branch.id))
    except KeycloakError as exc:
        if getattr(exc, "response_code", None) == 404:
            logger.error("Keycloak realm not found for branch %s during delete; continuing.", branch.id, exc_info=True)
        else:
            raise
    await delete_branch_provisioning(session, branch)
    await session.delete(branch)
    await session.commit()

    return Response(status_code=204)


@instance_api.post(
    "/reset-password",
    name="organizations:projects:branch:reset-password",
    status_code=204,
    responses={
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        500: {"description": "Failed to rotate branch database password."},
    },
)
async def reset_password(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
    parameters: BranchPasswordReset,
) -> Response:
    admin_password = branch.database_password
    db_host = branch.endpoint_domain or branch_domain(branch.id)
    if not db_host:
        db_host = get_deployment_settings().deployment_host
    if not db_host:
        logging.error("Database host unavailable for branch %s", branch.id)
        raise HTTPException(status_code=500, detail="Branch database host is not configured.")

    try:
        await update_branch_database_password(
            branch_id=branch.id,
            database=branch.database,
            username=branch.database_user,
            admin_password=admin_password,
            new_password=parameters.new_password,
        )
    except (asyncpg_exceptions.PostgresError, OSError) as exc:
        logging.exception("Failed to rotate database password for branch %s", branch.id)
        raise HTTPException(status_code=500, detail="Failed to rotate branch database password.") from exc

    await session.commit()
    return Response(status_code=204)


# PgBouncer configuration
@instance_api.get(
    "/pgbouncer-config",
    name="organizations:projects:branch:get-pgbouncer-config",
    response_model=BranchPgbouncerConfigStatus,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def get_pgbouncer_config(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> BranchPgbouncerConfigStatus:
    config = await branch.awaitable_attrs.pgbouncer_config
    config_snapshot = snapshot_pgbouncer_config(config)

    namespace, _ = get_db_vmi_identity(branch.id)
    pgbouncer_status = await _probe_service_socket(
        host=_pgbouncer_host_for_namespace(namespace),
        port=_PGBOUNCER_SERVICE_PORT,
        label="pgbouncer",
    )

    return BranchPgbouncerConfigStatus(
        pgbouncer_enabled=config is not None,
        pgbouncer_status=pgbouncer_status,
        pool_mode="transaction",
        max_client_conn=config_snapshot["max_client_conn"],
        default_pool_size=config_snapshot["default_pool_size"],
        server_idle_timeout=config_snapshot["server_idle_timeout"],
        server_lifetime=config_snapshot["server_lifetime"],
        query_wait_timeout=config_snapshot["query_wait_timeout"],
        reserve_pool_size=config_snapshot["reserve_pool_size"],
    )


@instance_api.patch(
    "/pgbouncer-config",
    name="organizations:projects:branch:update-pgbouncer-config",
    response_model=BranchPgbouncerConfigStatus,
    responses={
        401: Unauthenticated,
        403: Forbidden,
        404: NotFound,
        500: {"description": "PgBouncer configuration template missing required entries."},
        502: {"description": "Failed to update PgBouncer configuration."},
    },
)
async def update_pgbouncer_config(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
    parameters: BranchPgbouncerConfigUpdate,
) -> BranchPgbouncerConfigStatus:
    config = _ensure_pgbouncer_config(session, branch)

    namespace, vmi_name = get_db_vmi_identity(branch.id)
    host = _pgbouncer_host_for_namespace(namespace)
    update_commands = _collect_pgbouncer_updates(parameters)

    if not update_commands:
        raise HTTPException(status_code=400, detail="No PgBouncer parameters provided for update.")

    config_map_name = f"{vmi_name}-pgbouncer"
    try:
        await _update_pgbouncer_config_map(
            namespace=namespace,
            config_map_name=config_map_name,
            updates=update_commands,
        )
    except HTTPException:
        await session.rollback()
        raise

    admin_password = _resolve_pgbouncer_password(branch)

    try:
        await _apply_pgbouncer_settings(host=host, password=admin_password, update_commands=update_commands)
    except HTTPException:
        await session.rollback()
        raise

    _persist_pgbouncer_settings(config, update_commands)
    await session.commit()
    await session.refresh(config)

    return BranchPgbouncerConfigStatus(
        pgbouncer_enabled=True,
        pgbouncer_status="RELOADING",
        pool_mode="transaction",
        max_client_conn=config.max_client_conn,
        default_pool_size=config.default_pool_size,
        query_wait_timeout=config.query_wait_timeout,
        reserve_pool_size=config.reserve_pool_size,
        server_idle_timeout=config.server_idle_timeout,
        server_lifetime=config.server_lifetime,
    )


# Resize controls
@instance_api.post(
    "/resize",
    name="organizations:projects:branch:resize",
    status_code=202,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def resize(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    parameters: ResizeParameters,
    branch: BranchDep,
):
    branch_in_session = await session.merge(branch)

    if parameters.storage_size is not None:
        current_storage = branch_in_session.storage_size
        requested_storage = parameters.storage_size
        if current_storage is not None and requested_storage < current_storage:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Reducing branch storage is not supported. "
                    f"Current allocation is {current_storage} bytes, requested {requested_storage} bytes."
                ),
            )

    updated_statuses = dict(branch_in_session.resize_statuses or {})
    timestamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    effective_parameters: dict[CapaResizeKey, int] = {}

    _track_resize_change(
        parameter_key="database_size",
        new_value=parameters.database_size,
        current_value=branch_in_session.database_size,
        statuses=updated_statuses,
        effective=effective_parameters,
        timestamp=timestamp,
    )
    _track_resize_change(
        parameter_key="storage_size",
        new_value=parameters.storage_size,
        current_value=branch_in_session.storage_size,
        statuses=updated_statuses,
        effective=effective_parameters,
        timestamp=timestamp,
    )
    _track_resize_change(
        parameter_key="milli_vcpu",
        new_value=parameters.milli_vcpu,
        current_value=branch_in_session.milli_vcpu,
        statuses=updated_statuses,
        effective=effective_parameters,
        timestamp=timestamp,
    )
    _track_resize_change(
        parameter_key="memory_bytes",
        new_value=parameters.memory_bytes,
        current_value=branch_in_session.memory,
        statuses=updated_statuses,
        effective=effective_parameters,
        timestamp=timestamp,
    )
    _track_resize_change(
        parameter_key="iops",
        new_value=parameters.iops,
        current_value=branch_in_session.iops,
        statuses=updated_statuses,
        effective=effective_parameters,
        timestamp=timestamp,
    )

    if effective_parameters:
        await _ensure_resize_resource_limits(session, branch_in_session, effective_parameters)
        await _apply_resize_operations(session, branch_in_session, effective_parameters)
        completion_timestamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if "milli_vcpu" in effective_parameters:
            updated_statuses["database_cpu_resize"] = {
                "status": "COMPLETED",
                "timestamp": completion_timestamp,
            }
        if "iops" in effective_parameters:
            updated_statuses["database_iops_resize"] = {
                "status": "COMPLETED",
                "timestamp": completion_timestamp,
            }

    branch_in_session.resize_statuses = updated_statuses
    branch_in_session.resize_status = aggregate_resize_statuses(updated_statuses)
    await session.commit()
    return Response(status_code=202)


_control_responses: dict[int | str, dict[str, Any]] = {
    401: Unauthenticated,
    403: Forbidden,
    404: NotFound,
}

_CONTROL_TO_KUBEVIRT: dict[str, KubevirtSubresourceAction] = {
    "pause": "pause",
    "resume": "unpause",
    "start": "start",
    "stop": "stop",
}

_CONTROL_TRANSITION_INITIAL: dict[str, BranchServiceStatus] = {
    "pause": BranchServiceStatus.PAUSING,
    "resume": BranchServiceStatus.RESUMING,
    "start": BranchServiceStatus.STARTING,
    "stop": BranchServiceStatus.STOPPING,
}

_CONTROL_TRANSITION_FINAL: dict[str, BranchServiceStatus | None] = {
    "pause": BranchServiceStatus.PAUSED,
    "resume": BranchServiceStatus.STARTING,
    "start": None,
    "stop": BranchServiceStatus.STOPPED,
}


@instance_api.post(
    "/pause",
    name="organizations:projects:branch:pause",
    status_code=204,
    responses=_control_responses,
)
@instance_api.post(
    "/resume",
    name="organizations:projects:branch:resume",
    status_code=204,
    responses=_control_responses,
)
@instance_api.post(
    "/start",
    name="organizations:projects:branch:start",
    status_code=204,
    responses=_control_responses,
)
@instance_api.post(
    "/stop",
    name="organizations:projects:branch:stop",
    status_code=204,
    responses=_control_responses,
)
async def control_branch(
    session: SessionDep,
    request: Request,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
):
    action = request.scope["route"].name.split(":")[-1]
    assert action in _CONTROL_TO_KUBEVIRT
    namespace, vmi_name = get_db_vmi_identity(branch.id)
    branch_in_session = await session.merge(branch)
    branch_id = branch_in_session.id
    branch_milli_vcpu = branch_in_session.milli_vcpu
    initial_status = _CONTROL_TRANSITION_INITIAL[action]
    if _apply_local_branch_status(branch_in_session, initial_status):
        await session.commit()
    try:
        await call_kubevirt_subresource(namespace, vmi_name, _CONTROL_TO_KUBEVIRT[action])
        if action == "start":

            async def _run_cpu_sync() -> None:
                try:
                    await _sync_branch_cpu_resources(
                        branch_id,
                        desired_milli_vcpu=branch_milli_vcpu,
                    )
                except VelaKubernetesError:
                    logger.exception("Failed to sync CPU resources after starting branch %s", branch_id)

            asyncio.create_task(_run_cpu_sync())
    except ApiException as e:
        if _apply_local_branch_status(branch_in_session, BranchServiceStatus.ERROR):
            await session.commit()
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e
    else:
        final_status = _CONTROL_TRANSITION_FINAL[action]
        if final_status is not None and _apply_local_branch_status(branch_in_session, final_status):
            await session.commit()
    return Response(status_code=204)


instance_api.include_router(auth_api, prefix="/auth")


@instance_api.get(
    "/apikeys",
    name="organizations:projects:branch:apikeys",
    response_model=list[ApiKeyDetails],
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def get_apikeys(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> list[ApiKeyDetails]:
    if not branch.anon_key or not branch.service_key:
        raise HTTPException(status_code=404, detail="API keys not found for this branch")

    def create_key_details(name: str, key: str, description: str) -> ApiKeyDetails:
        return ApiKeyDetails(
            name=name,
            api_key=key,
            id=name,
            hash=hashlib.sha256(key.encode()).hexdigest(),
            prefix=key[:5],
            description=description,
        )

    return [
        create_key_details("anon", branch.anon_key, "Legacy anon API key"),
        create_key_details("service_role", branch.service_key, "Legacy service_role API key"),
    ]


api.include_router(instance_api)


def _ensure_pgbouncer_config(session: SessionDep, branch: Branch) -> PgbouncerConfig:
    config = branch.pgbouncer_config
    if config is None:
        config = PgbouncerConfig(
            default_pool_size=PgbouncerConfig.DEFAULT_POOL_SIZE,
            max_client_conn=PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN,
            query_wait_timeout=PgbouncerConfig.DEFAULT_QUERY_WAIT_TIMEOUT,
            reserve_pool_size=PgbouncerConfig.DEFAULT_RESERVE_POOL_SIZE,
            server_idle_timeout=PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT,
            server_lifetime=PgbouncerConfig.DEFAULT_SERVER_LIFETIME,
        )
        branch.pgbouncer_config = config
        session.add(config)
    return config


def _collect_pgbouncer_updates(parameters: BranchPgbouncerConfigUpdate) -> dict[str, int]:
    updates: dict[str, int] = {}
    if parameters.default_pool_size is not None:
        updates["default_pool_size"] = parameters.default_pool_size
    if parameters.max_client_conn is not None:
        updates["max_client_conn"] = parameters.max_client_conn
    if parameters.server_idle_timeout is not None:
        updates["server_idle_timeout"] = parameters.server_idle_timeout
    if parameters.server_lifetime is not None:
        updates["server_lifetime"] = parameters.server_lifetime
    if parameters.query_wait_timeout is not None:
        updates["query_wait_timeout"] = parameters.query_wait_timeout
    if parameters.reserve_pool_size is not None:
        updates["reserve_pool_size"] = parameters.reserve_pool_size
    return updates


def _pgbouncer_host_for_namespace(namespace: str) -> str:
    return f"{get_deployment_settings().deployment_release_name}-pgbouncer.{namespace}.svc.cluster.local"


def _resolve_pgbouncer_password(branch: Branch) -> str:
    try:
        return branch.pgbouncer_password
    except ValueError as exc:
        logger.exception("PgBouncer admin password missing for branch %s", branch.id)
        raise HTTPException(status_code=500, detail="PgBouncer admin password is unavailable.") from exc


async def _update_pgbouncer_config_map(
    *,
    namespace: str,
    config_map_name: str,
    updates: dict[str, int],
) -> None:
    if not updates:
        return

    async with core_v1_client() as core_v1:
        try:
            config_map = await core_v1.read_namespaced_config_map(name=config_map_name, namespace=namespace)
        except ApiException as exc:
            if exc.status == 404:
                logger.exception(
                    "PgBouncer ConfigMap %s/%s not found while applying updates",
                    namespace,
                    config_map_name,
                )
                raise HTTPException(status_code=500, detail=_PGBOUNCER_CONFIG_TEMPLATE_ERROR) from exc
            logger.exception(
                "Failed to retrieve PgBouncer ConfigMap %s/%s",
                namespace,
                config_map_name,
            )
            raise HTTPException(status_code=502, detail=_PGBOUNCER_CONFIG_UPDATE_ERROR) from exc

        data = dict(config_map.data or {})
        try:
            ini_content = data["pgbouncer.ini"]
        except KeyError as exc:
            logger.exception(
                "PgBouncer ConfigMap %s/%s missing pgbouncer.ini entry",
                namespace,
                config_map_name,
            )
            raise HTTPException(status_code=500, detail=_PGBOUNCER_CONFIG_TEMPLATE_ERROR) from exc

        try:
            updated_ini = _render_updated_pgbouncer_ini(ini_content, updates)
        except ValueError as exc:
            logger.exception(
                "PgBouncer ConfigMap %s/%s missing required PgBouncer settings: %s",
                namespace,
                config_map_name,
                exc,
            )
            raise HTTPException(status_code=500, detail=_PGBOUNCER_CONFIG_TEMPLATE_ERROR) from exc

        data["pgbouncer.ini"] = updated_ini
        data["namespace"] = namespace

        patch_body = {"data": data}
        try:
            await core_v1.patch_namespaced_config_map(
                name=config_map_name,
                namespace=namespace,
                body=patch_body,
            )
        except ApiException as exc:
            logger.exception(
                "Failed to patch PgBouncer ConfigMap %s/%s",
                namespace,
                config_map_name,
            )
            raise HTTPException(status_code=502, detail=_PGBOUNCER_CONFIG_UPDATE_ERROR) from exc


def _render_updated_pgbouncer_ini(content: str, updates: dict[str, int]) -> str:
    if not updates:
        return content

    lines = content.splitlines(keepends=True)
    for setting, value in updates.items():
        replaced = False
        for index, line in enumerate(lines):
            stripped = line.lstrip()
            if not stripped.startswith(f"{setting} ="):
                continue

            line_without_newline = line.rstrip("\r\n")
            line_ending = line[len(line_without_newline) :]
            leading_length = len(line_without_newline) - len(line_without_newline.lstrip())
            indent = line_without_newline[:leading_length]

            lines[index] = f"{indent}{setting} = {value}{line_ending}"
            replaced = True
            break

        if not replaced:
            raise ValueError(f"missing setting {setting!r}")

    return "".join(lines)


async def _apply_pgbouncer_settings(*, host: str, password: str, update_commands: dict[str, int]) -> None:
    connection: asyncpg.Connection | None = None
    try:
        connection = await asyncpg.connect(
            user=_PGBOUNCER_ADMIN_USER,
            password=password,
            database=_PGBOUNCER_ADMIN_DATABASE,
            host=host,
            port=_PGBOUNCER_SERVICE_PORT,
            server_settings={"application_name": "vela-pgbouncer-config"},
            command_timeout=10,
        )
        for setting, value in update_commands.items():
            await connection.execute(f"SET {setting} = {value}")
        await connection.execute("RELOAD")
    except (asyncpg_exceptions.PostgresError, OSError) as exc:
        logger.exception("Failed to apply PgBouncer runtime settings for host %s", host)
        raise HTTPException(status_code=502, detail="Failed to apply PgBouncer runtime settings.") from exc
    finally:
        with contextlib.suppress(Exception):
            if connection is not None:
                await connection.close()


def _persist_pgbouncer_settings(config: PgbouncerConfig, updates: dict[str, int]) -> None:
    for field, value in updates.items():
        setattr(config, field, value)
