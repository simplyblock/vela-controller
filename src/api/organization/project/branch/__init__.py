import asyncio
import contextlib
import hashlib
import logging
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, TypedDict, cast

import asyncpg
from asyncpg import exceptions as asyncpg_exceptions
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from keycloak.exceptions import KeycloakError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from ....._util import DEFAULT_DB_NAME, DEFAULT_DB_USER, Identifier
from .....check_branch_status import get_branch_status
from .....deployment import (
    DeploymentParameters,
    ResizeParameters,
    branch_api_domain,
    branch_domain,
    branch_rest_endpoint,
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
from .....deployment.kubernetes.volume_clone import clone_branch_database_volume
from .....deployment.settings import settings as deployment_settings
from .....exceptions import VelaError, VelaKubernetesError
from ...._util import Conflict, Forbidden, NotFound, Unauthenticated, url_path_for
from ...._util.backups import copy_branch_backup_schedules
from ...._util.crypto import encrypt_with_passphrase, generate_keys
from ...._util.resourcelimit import (
    check_available_resources_limits,
    create_or_update_branch_provisioning,
    delete_branch_provisioning,
    format_limit_violation_details,
    get_current_branch_allocations,
)
from ...._util.role import clone_user_role_assignment
from ....auth import security
from ....db import SessionDep
from ....keycloak import realm_admin
from ....models.branch import (
    ApiKeyDetails,
    Branch,
    BranchApiKeys,
    BranchCreate,
    BranchDep,
    BranchPasswordReset,
    BranchPgbouncerConfigStatus,
    BranchPgbouncerConfigUpdate,
    BranchPublic,
    BranchResizeService,
    BranchResizeStatusEntry,
    BranchServiceStatus,
    BranchStatus,
    BranchUpdate,
    CapaResizeKey,
    DatabaseInformation,
    PgbouncerConfig,
    ResourceUsageDefinition,
    aggregate_resize_statuses,
)
from ....models.branch import (
    lookup as lookup_branch,
)
from ....models.organization import OrganizationDep
from ....models.project import ProjectDep
from ....models.resources import ResourceLimitsPublic
from ....settings import settings
from .auth import api as auth_api

api = APIRouter(tags=["branch"])

logger = logging.getLogger(__name__)

_SERVICE_PROBE_TIMEOUT_SECONDS = 2
_SNAPSHOT_TIMEOUT_SECONDS = float(600)
_SNAPSHOT_POLL_INTERVAL_SECONDS = float(2)
_PVC_TIMEOUT_SECONDS = float(600)
_PVC_POLL_INTERVAL_SECONDS = float(2)
_VOLUME_SNAPSHOT_CLASS = "simplyblock-csi-snapshotclass"

_BRANCH_SERVICE_ENDPOINTS: dict[str, tuple[str, int]] = {
    "database": ("supabase-supabase-db", 5432),
    "pgbouncer": ("supabase-pgbouncer", 6432),
    "realtime": ("supabase-supabase-realtime", 4000),
    "storage": ("supabase-supabase-storage", 5000),
    "meta": ("supabase-supabase-meta", 8080),
    "rest": ("supabase-supabase-rest", 3000),
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
        server_idle_timeout=config.server_idle_timeout,
        server_lifetime=config.server_lifetime,
    )


def _default_pgbouncer_config() -> PgbouncerConfig:
    return PgbouncerConfig(
        default_pool_size=PgbouncerConfig.DEFAULT_POOL_SIZE,
        max_client_conn=PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN,
        server_idle_timeout=PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT,
        server_lifetime=PgbouncerConfig.DEFAULT_SERVER_LIFETIME,
    )


class PgbouncerConfigSnapshot(TypedDict):
    default_pool_size: int
    max_client_conn: int | None
    server_idle_timeout: int | None
    server_lifetime: int | None


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
    return PgbouncerConfigSnapshot(
        default_pool_size=config.default_pool_size,
        max_client_conn=max_client_conn,
        server_idle_timeout=server_idle_timeout,
        server_lifetime=server_lifetime,
    )


def pgbouncer_snapshot_to_mapping(snapshot: PgbouncerConfigSnapshot) -> dict[str, int]:
    max_client_conn = snapshot["max_client_conn"]
    if max_client_conn is None:
        default_max = PgbouncerConfig.DEFAULT_MAX_CLIENT_CONN
        if default_max is None:
            raise ValueError("PgBouncer default max_client_conn is not configured")
        max_client_conn = default_max

    server_idle_timeout = snapshot["server_idle_timeout"]
    if server_idle_timeout is None:
        default_idle = PgbouncerConfig.DEFAULT_SERVER_IDLE_TIMEOUT
        if default_idle is None:
            raise ValueError("PgBouncer default server_idle_timeout is not configured")
        server_idle_timeout = default_idle

    server_lifetime = snapshot["server_lifetime"]
    if server_lifetime is None:
        default_lifetime = PgbouncerConfig.DEFAULT_SERVER_LIFETIME
        if default_lifetime is None:
            raise ValueError("PgBouncer default server_lifetime is not configured")
        server_lifetime = default_lifetime

    return {
        "default_pool_size": snapshot["default_pool_size"],
        "max_client_conn": max_client_conn,
        "server_idle_timeout": server_idle_timeout,
        "server_lifetime": server_lifetime,
    }


def _deployment_parameters_from_source(source: Branch) -> DeploymentParameters:
    image_tag = source.database_image_tag
    if image_tag != "15.1.0.147":  # pragma: no cover - defensive guard against unsupported images
        logger.warning(
            "Source branch %s has unexpected database image tag %s; defaulting to supported image",
            source.id,
            image_tag,
        )
        image_tag = "15.1.0.147"

    return DeploymentParameters(
        database_password=source.database_password,
        database_size=source.database_size,
        storage_size=source.storage_size,
        milli_vcpu=source.milli_vcpu,
        memory_bytes=source.memory,
        iops=source.iops,
        database_image_tag=cast("Literal['15.1.0.147']", image_tag),
        enable_file_storage=source.enable_file_storage,
    )


async def _build_branch_entity(
    *,
    project: ProjectDep,
    parameters: BranchCreate,
    source: Branch | None,
    copy_config: bool,
) -> tuple[Branch, DeploymentParameters | None]:
    if source is not None:
        env_type = parameters.env_type if parameters.env_type is not None else ""
        entity = Branch(
            name=parameters.name,
            project_id=project.id,
            parent_id=source.id,
            database=DEFAULT_DB_NAME,
            database_user=DEFAULT_DB_USER,
            database_size=source.database_size,
            storage_size=source.storage_size,
            milli_vcpu=source.milli_vcpu,
            memory=source.memory,
            iops=source.iops,
            database_image_tag=source.database_image_tag,
            env_type=env_type,
            enable_file_storage=source.enable_file_storage,
        )
        entity.database_password = source.database_password
        if copy_config:
            entity.pgbouncer_config = await _copy_pgbouncer_config_from_source(source)
        else:
            entity.pgbouncer_config = _default_pgbouncer_config()
        return entity, _deployment_parameters_from_source(source)

    deployment_params = cast("DeploymentParameters", parameters.deployment)
    entity = Branch(
        name=parameters.name,
        project_id=project.id,
        parent=None,
        database=DEFAULT_DB_NAME,
        database_user=DEFAULT_DB_USER,
        database_size=deployment_params.database_size,
        storage_size=deployment_params.storage_size,
        milli_vcpu=deployment_params.milli_vcpu,
        memory=deployment_params.memory_bytes,
        iops=deployment_params.iops,
        database_image_tag=deployment_params.database_image_tag,
        enable_file_storage=deployment_params.enable_file_storage,
    )
    entity.database_password = deployment_params.database_password
    entity.pgbouncer_config = _default_pgbouncer_config()
    return entity, None


async def _probe_service_socket(host: str, port: int, *, label: str) -> BranchServiceStatus:
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host=host, port=port),
            timeout=_SERVICE_PROBE_TIMEOUT_SECONDS,
        )
    except (TimeoutError, OSError):
        logger.debug("Service %s unavailable at %s:%s", label, host, port)
        return "STOPPED"
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Unexpected error probing service %s", label)
        return "UNKNOWN"

    writer.close()
    try:
        await writer.wait_closed()
    except OSError:  # pragma: no cover - best effort socket cleanup
        logger.debug("Failed to close probe socket for %s", label, exc_info=True)
    return "ACTIVE_HEALTHY"


async def _collect_branch_service_health(namespace: str, *, storage_enabled: bool) -> BranchStatus:
    endpoints = {
        label: (service_name, port)
        for label, (service_name, port) in _BRANCH_SERVICE_ENDPOINTS.items()
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
            results[label] = "UNKNOWN"

    return BranchStatus(
        database=results["database"],
        storage=results.get("storage", "STOPPED" if not storage_enabled else "UNKNOWN"),
        realtime=results["realtime"],
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


async def _apply_resize_operations(branch: Branch, effective_parameters: dict[CapaResizeKey, int]) -> None:
    resize_params = ResizeParameters(**{str(key): value for key, value in effective_parameters.items()})
    resize_deployment(branch.id, resize_params)

    if "iops" in effective_parameters:
        await update_branch_volume_iops(branch.id, effective_parameters["iops"])

    # TODO: as a part of memory monitor, after memory resize is complete, run _sync_branch_cpu_resources
    milli_vcpu = effective_parameters.get("milli_vcpu")
    if milli_vcpu is not None:
        await _sync_branch_cpu_resources(
            branch.id,
            desired_milli_vcpu=milli_vcpu,
        )
        branch.milli_vcpu = milli_vcpu


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
        logging.exception(
            "Branch deployment failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )


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
        logging.exception(
            "Branch deployment (clone) failed for project_id=%s branch_id=%s branch_slug=%s",
            project_id,
            branch_id,
            branch_slug,
        )


async def _public(branch: Branch) -> BranchPublic:
    project = await branch.awaitable_attrs.project

    db_host = branch.endpoint_domain or branch_domain(branch.id)
    if not db_host:
        db_host = deployment_settings.deployment_host
    port = 5432

    # pg-meta and pg are in the same network. So password is not required in connection string.
    connection_string = "postgresql://{user}@{host}:{port}/{database}".format(  # noqa: UP032
        user=branch.database_user,
        host="db",
        port=port,
        database="postgres",
    )

    rest_endpoint = branch_rest_endpoint(branch.id)
    api_domain = branch_api_domain(branch.id)

    if rest_endpoint:
        service_endpoint = rest_endpoint.removesuffix("/rest")
    elif api_domain:
        service_endpoint = f"https://{api_domain}"
    else:
        # Fall back to using the same host as the database when dedicated domains are unavailable.
        service_endpoint = f"https://{db_host}"

    max_resources = branch.provisioned_resources()

    database_info = DatabaseInformation(
        host=db_host,
        port=port,
        username=branch.database_user,
        name=branch.database,
        encrypted_connection_string=encrypt_with_passphrase(connection_string, settings.pgmeta_crypto_key),
        service_endpoint_uri=service_endpoint,
        version=branch.database_image_tag,
        has_replicas=False,
    )

    # FIXME: Replace placeholder telemetry data once usage metrics and labels are wired in.
    used_resources = ResourceUsageDefinition(
        milli_vcpu=0,
        ram_bytes=0,
        nvme_bytes=0,
        iops=0,
        storage_bytes=None,
    )
    namespace, _ = get_db_vmi_identity(branch.id)
    try:
        _service_status = await _collect_branch_service_health(namespace, storage_enabled=branch.enable_file_storage)
    except Exception:
        logging.exception("Failed to determine service health via socket probes")
        _service_status = BranchStatus(
            database="UNKNOWN",
            realtime="UNKNOWN",
            storage="STOPPED" if not branch.enable_file_storage else "UNKNOWN",
            meta="UNKNOWN",
            rest="UNKNOWN",
        )

    try:
        branch_status = await get_branch_status(branch.id)
    except Exception:
        logger.exception("Failed to determine branch status for %s", branch.id)
        branch_status = "UNKNOWN"

    api_keys = BranchApiKeys(anon=branch.anon_key, service_role=branch.service_key)

    normalized_resize_statuses: dict[str, BranchResizeStatusEntry] = {}
    for service, entry in (branch.resize_statuses or {}).items():
        if isinstance(entry, BranchResizeStatusEntry):
            normalized_resize_statuses[service] = entry
            continue
        try:
            normalized_resize_statuses[service] = BranchResizeStatusEntry.model_validate(entry)
        except ValidationError:
            logger.warning(
                "Skipping invalid resize status entry for branch %s service %s",
                branch.id,
                service,
            )

    return BranchPublic(
        id=branch.id,
        name=branch.name,
        project_id=branch.project_id,
        organization_id=project.organization_id,
        database=database_info,
        env_type=branch.env_type,
        max_resources=max_resources,
        resize_status=branch.resize_status,
        resize_statuses=normalized_resize_statuses,
        assigned_labels=[],
        used_resources=used_resources,
        api_keys=api_keys,
        status=branch_status,
        service_status=_service_status,
        pitr_enabled=False,
        created_at=branch.created_datetime,
        created_by="system",  # TODO: update it when user management is in place
        updated_at=None,
        updated_by=None,
    )


async def _build_resource_request(
    session: SessionDep, deployment_parameters: DeploymentParameters | None, source: Branch | None
) -> ResourceLimitsPublic:
    source_limits = await get_current_branch_allocations(session, source) if source is not None else None

    millis_vcpu = (
        deployment_parameters.milli_vcpu
        if deployment_parameters
        else source_limits.milli_vcpu
        if source_limits
        else None
    )

    ram = deployment_parameters.memory_bytes if deployment_parameters else source_limits.ram if source_limits else None
    iops = deployment_parameters.iops if deployment_parameters else source_limits.iops if source_limits else None

    database_size = (
        deployment_parameters.database_size
        if deployment_parameters
        else source_limits.database_size
        if source_limits
        else None
    )

    storage_size = (
        deployment_parameters.storage_size
        if deployment_parameters
        else source_limits.storage_size
        if source_limits
        else None
    )

    return ResourceLimitsPublic(
        milli_vcpu=millis_vcpu,
        ram=ram,
        iops=iops,
        database_size=database_size,
        storage_size=storage_size,
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
    # Either one must be set for a valid request (create or clone)
    if parameters.source is None and parameters.deployment is None:
        raise HTTPException(400, "Either source or deployment parameters must be provided")

    source = await lookup_branch(session, project, parameters.source.branch_id) if parameters.source else None
    source_id: Identifier | None = source.id if source is not None else None
    resource_requests = await _build_resource_request(session, parameters.deployment, source)
    exceeded_limits, remaining_limits = await check_available_resources_limits(
        session, project.organization_id, project.id, resource_requests
    )
    if exceeded_limits:
        violation_details = format_limit_violation_details(exceeded_limits, resource_requests, remaining_limits)
        raise HTTPException(422, f"New branch will exceed limit(s): {violation_details}")

    copy_config = parameters.source.config_copy if parameters.source else False
    copy_data = parameters.source.data_copy if parameters.source else False

    entity, clone_parameters = await _build_branch_entity(
        project=project,
        parameters=parameters,
        source=source,
        copy_config=copy_config,
    )
    jwt_secret, anon_key, service_key = generate_keys(str(entity.id))
    entity.jwt_secret = jwt_secret
    entity.anon_key = anon_key
    entity.service_key = service_key
    pgbouncer_admin_password = generate_pgbouncer_password()
    entity.pgbouncer_password = pgbouncer_admin_password
    session.add(entity)
    try:
        await realm_admin("master").a_create_realm({"realm": str(entity.id)})
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

    await session.refresh(entity)
    if source is not None and copy_config:
        await copy_branch_backup_schedules(session, source, entity)
        await clone_user_role_assignment(session, source, entity)
    pgbouncer_config_snapshot = snapshot_pgbouncer_config(await entity.awaitable_attrs.pgbouncer_config)

    # Configure allocations
    await create_or_update_branch_provisioning(session, entity, resource_requests)

    entity_url = url_path_for(
        request,
        "organizations:projects:branch:detail",
        organization_id=await organization.awaitable_attrs.id,
        project_id=await project.awaitable_attrs.id,
        branch_id=entity.id,
    )
    if parameters.deployment is not None:
        asyncio.create_task(
            _deploy_branch_environment_task(
                organization_id=organization.id,
                project_id=project.id,
                credential=credentials.credentials,
                branch_id=entity.id,
                branch_slug=entity.name,
                parameters=parameters.deployment,
                jwt_secret=entity.jwt_secret,
                anon_key=entity.anon_key,
                service_key=entity.service_key,
                pgbouncer_admin_password=pgbouncer_admin_password,
                pgbouncer_config=pgbouncer_config_snapshot,
            )
        )
    elif source_id is not None and clone_parameters is not None:
        asyncio.create_task(
            _clone_branch_environment_task(
                organization_id=organization.id,
                project_id=project.id,
                credential=credentials.credentials,
                branch_id=entity.id,
                branch_slug=entity.name,
                parameters=clone_parameters,
                jwt_secret=entity.jwt_secret,
                anon_key=entity.anon_key,
                service_key=entity.service_key,
                pgbouncer_admin_password=pgbouncer_admin_password,
                source_branch_id=source_id,
                copy_data=copy_data,
                pgbouncer_config=pgbouncer_config_snapshot,
            )
        )

    payload = (await _public(entity)).model_dump() if response == "full" else None

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
    _session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    _branch: BranchDep,
    _parameters: BranchUpdate,
):
    # TODO implement update logic
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
    if branch.name == Branch.DEFAULT_SLUG:
        raise HTTPException(400, "Default branch cannot be deleted")
    await delete_deployment(branch.id)
    await realm_admin("master").a_delete_realm(str(branch.id))
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
        db_host = deployment_settings.deployment_host
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

    if parameters.memory_bytes is not None:
        current_memory = branch_in_session.memory
        requested_memory = parameters.memory_bytes
        if current_memory is not None and requested_memory < current_memory:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Reducing branch memory is not supported. "
                    f"Current allocation is {current_memory} bytes, requested {requested_memory} bytes."
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
        await _apply_resize_operations(branch_in_session, effective_parameters)
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
    request: Request,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
):
    action = request.scope["route"].name.split(":")[-1]
    assert action in _CONTROL_TO_KUBEVIRT
    namespace, vmi_name = get_db_vmi_identity(branch.id)
    try:
        await call_kubevirt_subresource(namespace, vmi_name, _CONTROL_TO_KUBEVIRT[action])
        if action == "start":

            async def _run_cpu_sync() -> None:
                try:
                    await _sync_branch_cpu_resources(
                        branch.id,
                        desired_milli_vcpu=branch.milli_vcpu,
                    )
                except VelaKubernetesError:
                    logger.exception("Failed to sync CPU resources after starting branch %s", branch.id)

            asyncio.create_task(_run_cpu_sync())
        return Response(status_code=204)
    except ApiException as e:
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e


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
    return updates


def _pgbouncer_host_for_namespace(namespace: str) -> str:
    return f"{deployment_settings.deployment_release_name}-pgbouncer.{namespace}.svc.cluster.local"


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
