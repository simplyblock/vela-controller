import asyncio
import base64
import json
import logging
import subprocess
import tempfile
import textwrap
from collections.abc import Mapping
from importlib import resources
from pathlib import Path
from typing import Annotated, Any, Literal

import asyncpg
import httpx
import yaml
from cloudflare import AsyncCloudflare, CloudflareError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import BaseModel, Field, model_validator
from ulid import ULID

from .._util import (
    CPU_CONSTRAINTS,
    DATABASE_SIZE_CONSTRAINTS,
    IOPS_CONSTRAINTS,
    MEMORY_CONSTRAINTS,
    STORAGE_SIZE_CONSTRAINTS,
    DBPassword,
    Identifier,
    Slug,
    StatusType,
    bytes_to_gb,
    bytes_to_mib,
    check_output,
)
from ..exceptions import VelaCloudflareError, VelaDeployError, VelaDeploymentError, VelaKubernetesError
from .grafana import create_vela_grafana_obj, delete_vela_grafana_obj
from .kubernetes import KubernetesService
from .kubernetes.kubevirt import get_virtualmachine_status
from .logflare import create_branch_logflare_objects, delete_branch_logflare_objects
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()

DEFAULT_DATABASE_VM_NAME = "supabase-supabase-db"
CHECK_ENCRYPTED_HEADER_PLUGIN_NAME = "check-x-connection-encrypted"
APIKEY_JWT_PLUGIN_NAME = "apikey-jwt"
CPU_REQUEST_FRACTION = 0.25  # request = 25% of limit
SIMPLYBLOCK_NAMESPACE = "simplyblock"
SIMPLYBLOCK_CSI_CONFIGMAP = "simplyblock-csi-cm"
SIMPLYBLOCK_CSI_SECRET = "simplyblock-csi-secret"
DATABASE_PVC_SUFFIX = "-supabase-db-pvc"


def branch_storage_class_name(branch_id: Identifier) -> str:
    return f"sc-{str(branch_id).lower()}"


def deployment_namespace(branch_id: Identifier) -> str:
    """Return the Kubernetes namespace for a branch using `<prefix>-<branch_id>` format."""

    branch_value = str(branch_id).lower()
    prefix = settings.deployment_namespace_prefix
    if prefix:
        return f"{prefix}-{branch_value}"
    return branch_value


def deployment_branch(namespace: str) -> ULID:
    """Return the branch ULID for a given deployment namespace."""

    prefix = settings.deployment_namespace_prefix.strip().lower()
    normalized = namespace.strip().lower()
    if prefix:
        prefix_token = f"{prefix}-"
        if not normalized.startswith(prefix_token):
            raise VelaDeploymentError(
                f"Namespace '{namespace}' does not match deployment prefix '{prefix}'",
            )
        suffix = normalized[len(prefix_token) :]
    else:
        suffix = normalized
    try:
        return ULID.from_str(suffix.upper())
    except ValueError as e:
        raise VelaDeploymentError(f"Invalid branch ULID in namespace '{namespace}'") from e


def branch_dns_label(branch_id: Identifier) -> str:
    """Return the deterministic DNS label for a branch based on its ULID."""

    return str(branch_id).lower()


def branch_domain(branch_id: Identifier) -> str | None:
    """Return the database host domain for a branch."""

    suffix = settings.cloudflare_domain_suffix.strip()
    if not suffix:
        return None
    return f"db.{branch_dns_label(branch_id)}.{suffix}".lower()


def branch_api_domain(branch_id: Identifier) -> str | None:
    """Return the API host domain for a branch."""

    suffix = settings.cloudflare_domain_suffix.strip()
    if not suffix:
        return None
    return f"{branch_dns_label(branch_id)}.{suffix}".lower()


def branch_rest_endpoint(branch_id: Identifier) -> str | None:
    """Return the PostgREST endpoint URL for a branch, if domain settings are available."""

    domain = branch_api_domain(branch_id)
    if not domain:
        return None
    return f"https://{domain}/rest"


def _release_name(namespace: str) -> str:
    _ = namespace  # kept for call-site clarity; release name is namespace-independent
    return settings.deployment_release_name


def inject_branch_env(compose: dict[str, Any], branch_id: Identifier) -> dict[str, Any]:
    try:
        vector_service = compose["services"]["vector"]
    except KeyError as e:
        raise RuntimeError("Failed to inject branch env into compose file: missing services.vector") from e

    vector_env = vector_service.setdefault("environment", {})
    vector_env["LOGFLARE_PUBLIC_ACCESS_TOKEN"] = settings.logflare_public_access_token
    vector_env["NAMESPACE"] = settings.deployment_namespace_prefix
    vector_env["VELA_BRANCH"] = str(branch_id).lower()

    return compose


class DeploymentParameters(BaseModel):
    database_password: DBPassword
    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS)]
    storage_size: Annotated[int | None, Field(**STORAGE_SIZE_CONSTRAINTS)] = None
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS)]  # units of milli vCPU
    memory_bytes: Annotated[int, Field(**MEMORY_CONSTRAINTS)]
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS)]
    database_image_tag: Literal["15.1.0.147"]
    enable_file_storage: bool = True

    @model_validator(mode="after")
    def ensure_storage_requirements(self) -> "DeploymentParameters":
        if self.enable_file_storage and self.storage_size is None:
            raise ValueError("storage_size is required when file storage is enabled")
        return self


class DeploymentStatus(BaseModel):
    status: StatusType


def _build_storage_class_manifest(*, storage_class_name: str, iops: int, base_storage_class: Any) -> dict[str, Any]:
    provisioner = getattr(base_storage_class, "provisioner", None)
    if not provisioner:
        raise VelaKubernetesError("Base storage class missing provisioner")

    base_parameters = dict(getattr(base_storage_class, "parameters", {}) or {})
    cluster_id = base_parameters.get("cluster_id")
    if not cluster_id:
        raise VelaKubernetesError("Base storage class missing required parameter 'cluster_id'")

    parameters = {key: str(value) for key, value in base_parameters.items()}
    parameters.update(
        {
            "qos_rw_iops": str(iops),
            "qos_rw_mbytes": "0",
            "qos_r_mbytes": "0",
            "qos_w_mbytes": "0",
        }
    )

    allow_volume_expansion = getattr(base_storage_class, "allow_volume_expansion", None)
    volume_binding_mode = getattr(base_storage_class, "volume_binding_mode", None)
    reclaim_policy = getattr(base_storage_class, "reclaim_policy", None)
    mount_options = getattr(base_storage_class, "mount_options", None)

    manifest: dict[str, Any] = {
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {
            "name": storage_class_name,
        },
        "provisioner": provisioner,
        "parameters": parameters,
    }
    if reclaim_policy is not None:
        manifest["reclaimPolicy"] = reclaim_policy
    if volume_binding_mode is not None:
        manifest["volumeBindingMode"] = volume_binding_mode
    if allow_volume_expansion is not None:
        manifest["allowVolumeExpansion"] = bool(allow_volume_expansion)
    if mount_options:
        manifest["mountOptions"] = list(mount_options)

    return manifest


async def _load_simplyblock_credentials() -> tuple[str, str, str]:
    config_map = await kube_service.get_config_map(SIMPLYBLOCK_NAMESPACE, SIMPLYBLOCK_CSI_CONFIGMAP)
    config_data = (config_map.data or {}).get("config.json")
    if not config_data:
        raise VelaDeploymentError("ConfigMap simplyblock-csi-cm missing 'config.json'")
    try:
        config = json.loads(config_data)
    except (TypeError, ValueError) as exc:
        raise VelaDeploymentError("Failed to parse Simplyblock CSI config JSON") from exc

    cluster_cfg = config.get("simplybk")
    if not isinstance(cluster_cfg, dict):
        raise VelaDeploymentError("Simplyblock CSI config missing 'simplybk' section")

    endpoint = cluster_cfg.get("ip")
    cluster_id = cluster_cfg.get("uuid")
    if not endpoint or not cluster_id:
        raise VelaDeploymentError("Simplyblock CSI config missing required 'ip' or 'uuid'")

    secret = await kube_service.get_secret(SIMPLYBLOCK_NAMESPACE, SIMPLYBLOCK_CSI_SECRET)
    secret_blob = (secret.data or {}).get("secret.json")
    if not secret_blob:
        raise VelaDeploymentError("Secret simplyblock-csi-secret missing 'secret.json'")
    try:
        decoded_secret = base64.b64decode(secret_blob).decode()
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise VelaDeploymentError("Failed to decode Simplyblock CSI secret") from exc
    try:
        secret_json = json.loads(decoded_secret)
    except (TypeError, ValueError) as exc:
        raise VelaDeploymentError("Failed to parse Simplyblock CSI secret JSON") from exc

    secret_cfg = secret_json.get("simplybk")
    if not isinstance(secret_cfg, dict):
        raise VelaDeploymentError("Simplyblock CSI secret missing 'simplybk' section")
    cluster_secret = secret_cfg.get("secret")
    if not cluster_secret:
        raise VelaDeploymentError("Simplyblock CSI secret missing 'secret'")

    return endpoint.rstrip("/"), cluster_id, cluster_secret


async def _resolve_database_volume_identifiers(namespace: str) -> tuple[str, str | None]:
    pvc_name = f"{_release_name(namespace)}{DATABASE_PVC_SUFFIX}"
    pvc = await kube_service.get_persistent_volume_claim(namespace, pvc_name)
    pvc_spec = getattr(pvc, "spec", None)
    volume_name = getattr(pvc_spec, "volume_name", None) if pvc_spec else None
    if not volume_name:
        raise VelaDeploymentError(f"PersistentVolumeClaim {namespace}/{pvc_name} is not bound to a PersistentVolume")

    pv = await kube_service.get_persistent_volume(volume_name)
    pv_spec = getattr(pv, "spec", None)
    csi_spec = getattr(pv_spec, "csi", None) if pv_spec else None
    volume_attributes = getattr(csi_spec, "volume_attributes", None) if csi_spec else None
    if not isinstance(volume_attributes, dict):
        raise VelaDeploymentError(
            f"PersistentVolume {volume_name} missing CSI volume attributes; cannot resolve Simplyblock volume UUID"
        )
    volume_uuid = volume_attributes.get("uuid")
    volume_cluster_id = volume_attributes.get("cluster_id")
    if not volume_uuid:
        raise VelaDeploymentError(f"PersistentVolume {volume_name} missing 'uuid' attribute in CSI volume attributes")
    return volume_uuid, volume_cluster_id


async def update_branch_volume_iops(branch_id: Identifier, iops: int) -> None:
    namespace = deployment_namespace(branch_id)

    endpoint, cluster_id, cluster_secret = await _load_simplyblock_credentials()
    volume_uuid, pv_cluster_id = await _resolve_database_volume_identifiers(namespace)
    if pv_cluster_id and pv_cluster_id != cluster_id:
        raise VelaDeploymentError(
            f"Cluster ID mismatch for Simplyblock volume {volume_uuid!r}: PV reports {pv_cluster_id}, "
            f"but credentials reference {cluster_id}"
        )
    url = f"{endpoint}/lvol/{volume_uuid}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{cluster_id} {cluster_secret}",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.put(url, headers=headers, json={"max-rw-iops": iops})
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip() or exc.response.reason_phrase or str(exc)
        raise VelaDeploymentError(
            f"Simplyblock volume API rejected IOPS update for volume {volume_uuid!r}: {detail}"
        ) from exc
    except httpx.HTTPError as exc:
        raise VelaDeploymentError(f"Failed to reach Simplyblock volume API at {url!r}") from exc

    logger.info("Updated Simplyblock volume %s IOPS to %s using endpoint %s", volume_uuid, iops, endpoint)


async def ensure_branch_storage_class(branch_id: Identifier, *, iops: int) -> str:
    storage_class_name = branch_storage_class_name(branch_id)
    base_storage_class = await kube_service.get_storage_class("simplyblock-csi-sc")
    storage_class_manifest = _build_storage_class_manifest(
        storage_class_name=storage_class_name,
        iops=iops,
        base_storage_class=base_storage_class,
    )
    await kube_service.apply_storage_class(storage_class_manifest)
    return storage_class_name


def _require_asset(path: Path, description: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{description} not found at {path}")
    return path


def _load_compose_manifest(branch_id: Identifier) -> dict[str, Any]:
    compose_file_path = _require_asset(Path(__file__).with_name("compose.yml"), "docker-compose manifest")
    compose_content = yaml.safe_load(compose_file_path.read_text())
    if not isinstance(compose_content, dict):
        raise VelaDeploymentError("docker-compose manifest must be a mapping")
    return inject_branch_env(compose_content, branch_id)


def _configure_compose_storage(compose: dict[str, Any], *, enable_file_storage: bool) -> dict[str, Any]:
    services = compose.get("services")
    if not isinstance(services, dict):
        raise VelaDeploymentError("docker-compose manifest missing 'services' mapping")
    if not enable_file_storage:
        services.pop("storage", None)
    return compose


def _load_chart_values(chart_root: Any) -> dict[str, Any]:
    values_content = yaml.safe_load((chart_root / "values.yaml").read_text())
    if not isinstance(values_content, dict):
        raise VelaDeploymentError("Supabase chart values.yaml must be a mapping")
    return values_content


def _configure_vela_values(
    values_content: dict[str, Any],
    *,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    storage_class_name: str,
    use_existing_db_pvc: bool,
    pgbouncer_config: Mapping[str, int] | None,
    enable_file_storage: bool,
) -> dict[str, Any]:
    pgbouncer_values = values_content.setdefault("pgbouncer", {})
    pgbouncer_cfg = pgbouncer_values.setdefault("config", {})
    if pgbouncer_config:
        for key in ("default_pool_size", "max_client_conn", "server_idle_timeout", "server_lifetime"):
            value = pgbouncer_config.get(key)
            if value is not None:
                pgbouncer_cfg[key] = value

    db_spec = values_content.setdefault("db", {})
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag

    secrets = values_content.setdefault("secret", {})
    secrets.setdefault("jwt", {}).update(
        secret=jwt_secret,
        anonKey=anon_key,
        serviceKey=service_key,
    )
    secrets.update(pgmeta_crypto_key=settings.pgmeta_crypto_key)
    secrets.setdefault("db", {})["password"] = parameters.database_password
    secrets.setdefault("pgbouncer", {})["admin_password"] = pgbouncer_admin_password

    resource_cfg = db_spec.setdefault("resources", {})
    resource_cfg["guestMemory"] = f"{bytes_to_mib(parameters.memory_bytes)}Mi"

    cpu_limit, cpu_request = calculate_cpu_resources(parameters.milli_vcpu)
    resource_cfg["limits"] = {"cpu": cpu_limit}
    resource_cfg["requests"] = {"cpu": cpu_request}

    storage_spec = values_content.setdefault("storage", {})
    storage_persistence = storage_spec.setdefault("persistence", {})
    if parameters.storage_size is not None:
        storage_persistence["size"] = f"{bytes_to_gb(parameters.storage_size)}G"
    else:
        storage_persistence.pop("size", None)
    storage_persistence["storageClassName"] = storage_class_name
    storage_spec["enabled"] = enable_file_storage

    db_persistence = db_spec.setdefault("persistence", {})
    db_persistence["size"] = f"{bytes_to_gb(parameters.database_size)}G"
    if use_existing_db_pvc:
        db_persistence["create"] = False
    db_persistence["storageClassName"] = storage_class_name

    return values_content


async def create_vela_config(
    branch_id: Identifier,
    parameters: DeploymentParameters,
    branch: Slug,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    *,
    use_existing_db_pvc: bool = False,
    pgbouncer_config: Mapping[str, int] | None = None,
):
    namespace = deployment_namespace(branch_id)
    logging.info(
        "Creating Vela configuration for namespace: %s (branch %s, branch_id=%s)",
        namespace,
        branch,
        branch_id,
    )

    chart = resources.files(__package__) / "charts" / "supabase"
    compose_file = _configure_compose_storage(
        _load_compose_manifest(branch_id),
        enable_file_storage=parameters.enable_file_storage,
    )
    vector_file = _require_asset(Path(__file__).with_name("vector.yml"), "vector config file")
    pb_hba_conf = _require_asset(Path(__file__).with_name("pg_hba.conf"), "pg_hba.conf file")
    values_content = _load_chart_values(chart)

    storage_class_name = await ensure_branch_storage_class(branch_id, iops=parameters.iops)
    values_content = _configure_vela_values(
        values_content,
        parameters=parameters,
        jwt_secret=jwt_secret,
        anon_key=anon_key,
        service_key=service_key,
        pgbouncer_admin_password=pgbouncer_admin_password,
        storage_class_name=storage_class_name,
        use_existing_db_pvc=use_existing_db_pvc,
        pgbouncer_config=pgbouncer_config,
        enable_file_storage=parameters.enable_file_storage,
    )

    with (
        tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_values,
        tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as modified_compose,
    ):
        yaml.safe_dump(compose_file, modified_compose, default_flow_style=False)
        yaml.safe_dump(values_content, temp_values, default_flow_style=False)
        modified_compose.flush()
        temp_values.flush()

        try:
            await check_output(
                [
                    "helm",
                    "install",
                    _release_name(namespace),
                    str(chart),
                    "--namespace",
                    namespace,
                    "--create-namespace",
                    "--set-file",
                    f"composeYaml={modified_compose.name}",
                    "--set-file",
                    f"vectorYaml={vector_file}",
                    "--set-file",
                    f"pgHbaConf={pb_hba_conf}",
                    "-f",
                    temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create deployment: {e.stderr}")
            release_name = _release_name(namespace)
            try:
                await check_output(
                    ["helm", "uninstall", release_name, "-n", namespace],
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except subprocess.CalledProcessError as uninstall_err:
                stderr_msg = (uninstall_err.stderr or "").lower()
                if "release: not found" not in stderr_msg:
                    raise
                logger.info("Helm release %s not found during cleanup; continuing", release_name)
            raise


async def get_deployment_status(branch_id: Identifier) -> DeploymentStatus:
    status: StatusType
    try:
        namespace, vmi_name = get_db_vmi_identity(branch_id)
        status = await get_virtualmachine_status(namespace, vmi_name)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to get deployment status (returning UNKNOWN): {e}")
        status = "Unknown"
    return DeploymentStatus(status=status)


async def delete_deployment(branch_id: Identifier) -> None:
    namespace, _ = get_db_vmi_identity(branch_id)
    storage_class_name = branch_storage_class_name(branch_id)
    try:
        await delete_branch_logflare_objects(branch_id)
        await delete_vela_grafana_obj(branch_id)
        await kube_service.delete_namespace(namespace)
    except ApiException as exc:
        if exc.status == 404:
            logger.info("Namespace %s not found", namespace)
        else:
            raise
    try:
        await kube_service.delete_storage_class(storage_class_name)
    except ApiException as exc:
        if exc.status == 404:
            logger.info("StorageClass %s not found", storage_class_name)
        else:
            raise


def get_db_vmi_identity(branch_id: Identifier) -> tuple[str, str]:
    """
    Return the (namespace, vmi_name) for the project's database VirtualMachineInstance.

    The Helm chart defines the DB VM fullname as "{Release.Name}-{ChartName}-db" when no overrides
    are provided. With the configurable release name (`settings.deployment_release_name`, default
    "supabase") and chart name "supabase", the VMI resolves to
    f"{_release_name(namespace)}-supabase-db".
    """
    namespace = deployment_namespace(branch_id)
    vmi_name = f"{_release_name(namespace)}-supabase-db"
    return namespace, vmi_name


def calculate_cpu_resources(milli_vcpu: int) -> tuple[str, str]:
    """Return (limit, request) CPU quantities formatted for Kubernetes."""

    cpu_limit = f"{milli_vcpu}m"
    cpu_request_milli = max(1, int(milli_vcpu * CPU_REQUEST_FRACTION))
    cpu_request = f"{cpu_request_milli}m"
    return cpu_limit, cpu_request


class ResizeParameters(BaseModel):
    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS)] | None = None
    storage_size: Annotated[int, Field(**STORAGE_SIZE_CONSTRAINTS)] | None = None
    memory_bytes: Annotated[int, Field(**MEMORY_CONSTRAINTS)] | None = None
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS)] | None = None
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS)] | None = None

    @model_validator(mode="after")
    def ensure_at_least_one(self) -> "ResizeParameters":
        if (
            self.database_size is None
            and self.storage_size is None
            and self.memory_bytes is None
            and self.milli_vcpu is None
            and self.iops is None
        ):
            raise ValueError("Specify at least one of database_size, storage_size, memory_bytes, milli_vcpu, or iops")
        return self


def resize_deployment(branch_id: Identifier, parameters: ResizeParameters):
    """Perform an in-place Helm upgrade for resize operations. Only parameters provided will be
    updated; others are preserved using --reuse-values.
    """
    chart = resources.files(__package__) / "charts" / "supabase"
    # Minimal values file with only overrides
    values_content: dict[str, Any] = {}

    # resize Database volume
    if parameters.database_size is not None:
        db_spec = values_content.setdefault("db", {})
        db_spec.setdefault("persistence", {})["size"] = f"{bytes_to_gb(parameters.database_size)}G"

    # resize storageAPI volume
    if parameters.storage_size is not None:
        storage_size_gb = f"{bytes_to_gb(parameters.storage_size)}G"
        values_content.setdefault("storage", {}).setdefault("persistence", {})["size"] = storage_size_gb

    # resize memory
    if parameters.memory_bytes is not None:
        db_spec = values_content.setdefault("db", {})
        resource_cfg = db_spec.setdefault("resources", {})
        resource_cfg["guestMemory"] = f"{bytes_to_mib(parameters.memory_bytes)}Mi"

    if not values_content:
        logger.info("No Helm overrides required for resize of branch %s; skipping upgrade.", branch_id)
        return

    namespace = deployment_namespace(branch_id)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)
        subprocess.check_call(
            [
                "helm",
                "upgrade",
                _release_name(namespace),
                str(chart),
                "--namespace",
                namespace,
                "--reuse-values",
                "-f",
                temp_values.name,
            ]
        )


async def update_branch_database_password(
    *,
    branch_id: Identifier,
    database: str,
    username: str,
    admin_password: str,
    new_password: str,
    ssl: Any | None = None,
) -> None:
    """Rotate the admin credentials for a branch by connecting directly to the database."""

    connection: asyncpg.Connection | None = None
    namespace = deployment_namespace(branch_id)
    host: str = f"supabase-supabase-db.{namespace}.svc.cluster.local"
    try:
        connection = await asyncpg.connect(
            user="supabase_admin",  # superuser with permissions to change password
            password=admin_password,
            database=database,
            host=host,
            port=5432,
            ssl=ssl,
            server_settings={"application_name": "vela-password-rotation"},
            command_timeout=10,
        )
        # Postgres does not accept parameter placeholders in ALTER ROLE PASSWORD, so
        # generate a safely quoted statement via format().
        alter_sql = await connection.fetchval(
            "SELECT format('ALTER ROLE %I WITH PASSWORD %L', $1::text, $2::text)",
            username,
            new_password,
        )
        await connection.execute(alter_sql)
    finally:
        if connection is not None:
            await connection.close()


class CloudflareConfig(BaseModel):
    api_token: str
    zone_id: str
    branch_ref_cname: str
    domain_suffix: str


class KubeGatewayConfig(BaseModel):
    namespace: str = ""
    gateway_name: str = settings.gateway_name
    gateway_namespace: str = settings.gateway_namespace

    def for_namespace(self, namespace: str) -> "KubeGatewayConfig":
        return self.model_copy(update={"namespace": namespace})


class HTTPRouteSpec(BaseModel):
    ref: str
    domain: str
    namespace: str
    service_name: str
    service_port: int
    path_prefix: str
    route_suffix: str
    plugins: list[str] = Field(default_factory=lambda: ["cors"])


class BranchEndpointProvisionSpec(BaseModel):
    project_id: Identifier
    branch_id: Identifier
    branch_slug: str
    enable_file_storage: bool
    jwt_secret: str


class BranchEndpointResult(BaseModel):
    ref: str
    domain: str


async def _create_dns_record(cf: CloudflareConfig, domain: str) -> None:
    try:
        async with AsyncCloudflare(api_token=cf.api_token) as client:
            await client.dns.records.create(
                zone_id=cf.zone_id,
                name=domain,
                type="CNAME",
                content=cf.branch_ref_cname,
                ttl=1,  # Cloudflare API uses 1 to represent automatic TTL
                proxied=False,
            )
    except CloudflareError as exc:
        raise VelaCloudflareError(f"Cloudflare API error: {exc}") from exc
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaCloudflareError(f"Cloudflare request failed: {exc}") from exc

    logger.info("Created DNS CNAME record %s -> %s", domain, cf.branch_ref_cname)


def _build_http_route(cfg: KubeGatewayConfig, spec: HTTPRouteSpec) -> dict[str, Any]:
    annotations = {
        "konghq.com/strip-path": "true",
    }
    if spec.plugins:
        annotations["konghq.com/plugins"] = ",".join(spec.plugins)

    return {
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRoute",
        "metadata": {
            "name": f"{spec.ref}-{spec.route_suffix}",
            "namespace": spec.namespace,
            "annotations": annotations,
        },
        "spec": {
            "parentRefs": [
                {
                    "name": cfg.gateway_name,
                    "namespace": cfg.gateway_namespace,
                }
            ],
            "hostnames": [spec.domain],
            "rules": [
                {
                    "matches": [{"path": {"type": "PathPrefix", "value": spec.path_prefix}}],
                    "backendRefs": [
                        {
                            "name": spec.service_name,
                            "namespace": spec.namespace,
                            "port": spec.service_port,
                        }
                    ],
                }
            ],
        },
    }


def _postgrest_route_specs(ref: str, domain: str, namespace: str) -> list[HTTPRouteSpec]:
    """HTTPRoute definitions that expose the PostgREST service for a branch."""

    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name="supabase-supabase-rest",
            service_port=3000,
            path_prefix="/rest",
            route_suffix="postgrest-route",
            plugins=["cors", APIKEY_JWT_PLUGIN_NAME],
        ),
    ]


def _storage_route_specs(ref: str, domain: str, namespace: str) -> list[HTTPRouteSpec]:
    """HTTPRoute definitions that expose the Storage API service for a branch."""

    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name="supabase-supabase-storage",
            service_port=5000,
            path_prefix="/storage",
            route_suffix="storage-route",
            plugins=["cors", APIKEY_JWT_PLUGIN_NAME],
        ),
    ]


def _realtime_route_specs(ref: str, domain: str, namespace: str) -> list[HTTPRouteSpec]:
    """HTTPRoute definitions that expose the Realtime service for a branch."""

    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name="supabase-supabase-realtime",
            service_port=4000,
            path_prefix="/realtime",
            route_suffix="realtime-route",
        ),
    ]


def _pgmeta_route_specs(ref: str, domain: str, namespace: str) -> list[HTTPRouteSpec]:
    """HTTPRoute definitions that expose the Postgres Meta service for a branch."""

    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name="supabase-supabase-meta",
            service_port=8080,
            path_prefix="/pg-meta",
            route_suffix="pgmeta-route",
            plugins=["cors", CHECK_ENCRYPTED_HEADER_PLUGIN_NAME],
        ),
    ]


async def _apply_http_routes(namespace: str, routes: list[dict[str, Any]]) -> None:
    """Apply HTTPRoute manifests without blocking the event loop."""
    try:
        await kube_service.apply_http_routes(namespace, routes)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaKubernetesError(f"Failed to apply HTTPRoute: {exc}") from exc


def _build_kong_plugins(namespace: str) -> list[dict[str, Any]]:
    return [
        _build_cors_plugin(namespace),
        _build_check_encrypted_header_plugin(namespace),
        _build_apikey_jwt_plugin(namespace),
    ]


def _build_cors_plugin(namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "configuration.konghq.com/v1",
        "kind": "KongPlugin",
        "metadata": {
            "name": "cors",
            "namespace": namespace,
        },
        "config": {
            "origins": ["*"],  # todo: restrict this
            "methods": ["GET", "POST", "OPTIONS", "PUT", "DELETE"],
            "headers": ["*"],
            "exposed_headers": ["*"],
            "credentials": True,
        },
        "plugin": "cors",
    }


def _build_check_encrypted_header_plugin(namespace: str) -> dict[str, Any]:
    lua_script = textwrap.dedent(
        """
        local hdr = kong.request.get_header("x-connection-encrypted")
        if not hdr then
            return kong.response.exit(403, { message = "Missing x-connection-encrypted" })
        end
        """
    ).strip()

    return {
        "apiVersion": "configuration.konghq.com/v1",
        "kind": "KongPlugin",
        "metadata": {
            "name": CHECK_ENCRYPTED_HEADER_PLUGIN_NAME,
            "namespace": namespace,
        },
        "config": {
            "access": [lua_script],
        },
        "plugin": "pre-function",
    }


def _build_apikey_jwt_plugin(namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "configuration.konghq.com/v1",
        "kind": "KongPlugin",
        "metadata": {
            "name": APIKEY_JWT_PLUGIN_NAME,
            "namespace": namespace,
        },
        "plugin": "jwt",
        "config": {
            "header_names": ["apikey"],
            "uri_param_names": [],
            "cookie_names": [],
            "key_claim_name": "ref",
            "claims_to_verify": ["exp"],
            "secret_is_base64": False,
            "run_on_preflight": True,
        },
    }


def _consumer_resource_name(ref: str) -> str:
    return f"{ref}-jwt-consumer"


def _jwt_credential_secret_name(ref: str) -> str:
    return f"{ref}-jwt-credential"


def _build_kong_consumer(namespace: str, ref: str, branch_id: Identifier) -> dict[str, Any]:
    consumer_name = _consumer_resource_name(ref)
    return {
        "apiVersion": "configuration.konghq.com/v1",
        "kind": "KongConsumer",
        "metadata": {
            "name": consumer_name,
            "namespace": namespace,
        },
        "username": consumer_name,
        "custom_id": str(branch_id),
    }


def _build_kong_jwt_credential_secret(
    namespace: str,
    ref: str,
    branch_id: Identifier,
    jwt_secret: str,
) -> dict[str, Any]:
    credential_name = _jwt_credential_secret_name(ref)
    consumer_name = _consumer_resource_name(ref)
    branch_key = str(branch_id)
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": credential_name,
            "namespace": namespace,
            "annotations": {
                "konghq.com/credential": consumer_name,
            },
        },
        "type": "Opaque",
        "stringData": {
            "kongCredType": "jwt",
            "key": branch_key,
            "secret": jwt_secret,
            "algorithm": "HS256",
        },
    }


async def _apply_kong_plugin(namespace: str, plugin: dict[str, Any]) -> None:
    """Apply KongPlugin manifest without blocking the event loop."""
    try:
        await kube_service.apply_kong_plugin(namespace, plugin)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaKubernetesError(f"Failed to apply KongPlugin: {exc}") from exc


async def _apply_kong_consumer(namespace: str, consumer: dict[str, Any]) -> None:
    """Apply KongConsumer manifest without blocking the event loop."""
    try:
        await kube_service.apply_kong_consumer(namespace, consumer)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaKubernetesError(f"Failed to apply KongConsumer: {exc}") from exc


async def _apply_secret(namespace: str, secret: dict[str, Any]) -> None:
    """Apply Secret manifest without blocking the event loop."""
    try:
        await kube_service.apply_secret(namespace, secret)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaKubernetesError(f"Failed to apply Secret: {exc}") from exc


async def provision_branch_endpoints(
    spec: BranchEndpointProvisionSpec,
    *,
    ref: str,
) -> BranchEndpointResult:
    """Provision DNS + HTTPRoute resources (PostgREST + optional Storage + Realtime + PGMeta) for a branch."""

    cf_cfg = CloudflareConfig(
        api_token=settings.cloudflare_api_token,
        zone_id=settings.cloudflare_zone_id,
        branch_ref_cname=settings.cloudflare_branch_ref_cname,
        domain_suffix=settings.cloudflare_domain_suffix,
    )

    gateway_cfg = KubeGatewayConfig().for_namespace(deployment_namespace(spec.branch_id))

    domain = f"{ref}.{cf_cfg.domain_suffix}".lower()
    logger.info(
        "Provisioning endpoints for project_id=%s branch_id=%s branch_slug=%s domain=%s",
        spec.project_id,
        spec.branch_id,
        spec.branch_slug,
        domain,
    )

    await _create_dns_record(cf_cfg, domain)

    consumer_manifest = _build_kong_consumer(gateway_cfg.namespace, ref, spec.branch_id)
    await _apply_kong_consumer(gateway_cfg.namespace, consumer_manifest)

    credential_secret = _build_kong_jwt_credential_secret(
        gateway_cfg.namespace,
        ref,
        spec.branch_id,
        spec.jwt_secret,
    )
    await _apply_secret(gateway_cfg.namespace, credential_secret)

    # Apply the KongPlugins required for the branch routes
    kong_plugins = _build_kong_plugins(gateway_cfg.namespace)
    for plugin in kong_plugins:
        await _apply_kong_plugin(gateway_cfg.namespace, plugin)

    route_specs = _postgrest_route_specs(ref, domain, gateway_cfg.namespace)
    if spec.enable_file_storage:
        route_specs += _storage_route_specs(ref, domain, gateway_cfg.namespace)
    route_specs += _realtime_route_specs(ref, domain, gateway_cfg.namespace)
    route_specs += _pgmeta_route_specs(ref, domain, gateway_cfg.namespace)
    routes = [_build_http_route(gateway_cfg, route_spec) for route_spec in route_specs]
    await _apply_http_routes(gateway_cfg.namespace, routes)

    return BranchEndpointResult(ref=ref, domain=domain)


async def deploy_branch_environment(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: Slug,
    credential: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
    pgbouncer_admin_password: str,
    pgbouncer_config: Mapping[str, int],
    use_existing_pvc: bool = False,
) -> None:
    """Background task: provision infra for a branch and persist the resulting endpoint."""

    async def _serial_deploy():
        await create_vela_config(
            branch_id=branch_id,
            parameters=parameters,
            branch=branch_slug,
            jwt_secret=jwt_secret,
            anon_key=anon_key,
            service_key=service_key,
            pgbouncer_admin_password=pgbouncer_admin_password,
            use_existing_db_pvc=use_existing_pvc,
            pgbouncer_config=pgbouncer_config,
        )

        ref = branch_dns_label(branch_id)
        await provision_branch_endpoints(
            spec=BranchEndpointProvisionSpec(
                project_id=project_id,
                branch_id=branch_id,
                branch_slug=branch_slug,
                enable_file_storage=parameters.enable_file_storage,
                jwt_secret=jwt_secret,
            ),
            ref=ref,
        )

    results = await asyncio.gather(
        _serial_deploy(),
        create_branch_logflare_objects(branch_id=branch_id),
        create_vela_grafana_obj(
            organization_id, branch_id, credential
        ),  # FIXME: Fails with error: "certificate signed by unknown authority"
        return_exceptions=True,
    )

    if exceptions := [result for result in results if isinstance(result, Exception)]:
        raise VelaDeployError("Failed operations during vela deployment", exceptions)
