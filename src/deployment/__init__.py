import asyncio
import base64
import json
import logging
import math
import subprocess
import tempfile
import textwrap
from collections.abc import Mapping
from importlib import resources
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

import asyncpg
import httpx
import yaml
from cloudflare import AsyncCloudflare, CloudflareError
from kubernetes_asyncio import client as kubernetes_client
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import BaseModel, Field, model_validator
from ulid import ULID

from .._util import (
    AUTOSCALER_MEMORY_SLOT_SIZE_MIB,
    AUTOSCALER_MEMORY_SLOTS_MAX,
    AUTOSCALER_MEMORY_SLOTS_MIN,
    CPU_CONSTRAINTS,
    DATABASE_SIZE_CONSTRAINTS,
    IOPS_CONSTRAINTS,
    MEMORY_CONSTRAINTS,
    STORAGE_SIZE_CONSTRAINTS,
    VCPU_MILLIS_MAX,
    VCPU_MILLIS_MIN,
    Identifier,
    Name,
    bytes_to_gb,
    bytes_to_mib,
    check_output,
)
from ..exceptions import (
    VelaCloudflareError,
    VelaDeployError,
    VelaDeploymentError,
    VelaGrafanaError,
    VelaKubernetesError,
)
from ._util import deployment_namespace
from .deployment import DeploymentParameters, database_image_tag_to_database_images
from .grafana import create_vela_grafana_obj, delete_vela_grafana_obj
from .kubernetes import KubernetesService, get_neon_vm
from .kubernetes._util import custom_api_client
from .settings import CloudflareSettings, get_settings
from .simplyblock_api import create_simplyblock_api

if TYPE_CHECKING:
    from cloudflare.types.dns.record_list_params import Name as CloudflareRecordName

logger = logging.getLogger(__name__)

kube_service = KubernetesService()

CHART_NAME = "vela"


DEFAULT_DATABASE_VM_NAME = f"{CHART_NAME}-db"
DATABASE_CLUSTER_SERVICE_NAME = DEFAULT_DATABASE_VM_NAME
DATABASE_LOAD_BALANCER_SERVICE_NAME = f"{DEFAULT_DATABASE_VM_NAME}-ext"
CHECK_ENCRYPTED_HEADER_PLUGIN_NAME = "check-x-connection-encrypted"
APIKEY_JWT_PLUGIN_NAME = "apikey-jwt"
CPU_REQUEST_FRACTION = 0.25  # request = 25% of limit
SIMPLYBLOCK_NAMESPACE = "simplyblock"
SIMPLYBLOCK_CSI_CONFIGMAP = "simplyblock-csi-cm"
SIMPLYBLOCK_CSI_SECRET = "simplyblock-csi-secret"
STORAGE_PVC_SUFFIX = "-storage-pvc"
DATABASE_PVC_SUFFIX = "-db-pvc"
AUTOSCALER_PVC_SUFFIX = "-block-data"
_LOAD_BALANCER_TIMEOUT_SECONDS = float(600)
_LOAD_BALANCER_POLL_INTERVAL_SECONDS = float(2)
_OVERLAY_IP_TIMEOUT_SECONDS = float(300)
_OVERLAY_IP_POLL_INTERVAL_SECONDS = float(5)
_POD_SECURITY_LABELS = {
    "pod-security.kubernetes.io/enforce": "privileged",
    "pod-security.kubernetes.io/audit": "privileged",
    "pod-security.kubernetes.io/warn": "privileged",
}
DNSRecordType = Literal["AAAA", "CNAME"]
# TODO: Autoscaler VM's overlay IP is currently IPv4 only.
# https://github.com/simplyblock/vela/issues/347
DATABASE_DNS_RECORD_TYPE: Literal["CNAME"] = "CNAME"


def branch_storage_class_name(branch_id: Identifier) -> str:
    return f"sc-{str(branch_id).lower()}"


def deployment_branch(namespace: str) -> ULID:
    """Return the branch ULID for a given deployment namespace."""

    prefix = get_settings().deployment_namespace_prefix.strip().lower()
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


def branch_db_domain(branch_id: Identifier) -> str:
    """Return the database host domain for a branch."""

    suffix = get_settings().cloudflare.domain_suffix.strip()
    if not suffix:
        raise VelaDeploymentError("cloudflare dns domain suffix not configured")
    return f"db.{branch_dns_label(branch_id)}.{suffix}".lower()


def branch_api_domain(branch_id: Identifier) -> str | None:
    """Return the API host domain for a branch."""

    suffix = get_settings().cloudflare.domain_suffix.strip()
    if not suffix:
        return None
    return f"{branch_dns_label(branch_id)}.{suffix}".lower()


def branch_rest_endpoint(branch_id: Identifier) -> str | None:
    """Return the PostgREST endpoint URL for a branch, if domain settings are available."""

    domain = branch_api_domain(branch_id)
    if not domain:
        return None
    port = get_settings().deployment_service_port
    if port == 443:
        return f"https://{domain}/rest"
    return f"https://{domain}:{port}/rest"


def _release_name() -> str:
    return get_settings().deployment_release_name


def _release_fullname() -> str:
    release = _release_name()
    return release if CHART_NAME in release else f"{release}-{CHART_NAME}"


def _autoscaler_vm_name() -> str:
    name = f"{_release_fullname()}-autoscaler-vm"
    return name[:63].rstrip("-")


def branch_service_name(component: str) -> str:
    return f"{_release_fullname()}-{component}"


async def _wait_for_autoscaler_overlay_ip(namespace: str, vm_name: str) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _OVERLAY_IP_TIMEOUT_SECONDS
    last_error: Exception | None = None
    logger.info("Waiting for overlay IP for autoscaler VM %s/%s", namespace, vm_name)

    while True:
        try:
            vm = await get_neon_vm(namespace, vm_name)
        except (VelaKubernetesError, RuntimeError) as exc:
            last_error = exc
            vm = None

        if vm:
            overlay_ip = (vm.status.extra_net_ip or "").strip()
            if overlay_ip:
                logger.info(
                    "Autoscaler VM %s/%s overlay network %s is ready",
                    namespace,
                    vm_name,
                    overlay_ip,
                )
                return overlay_ip

        if loop.time() >= deadline:
            message = f"Timed out waiting for overlay IP for autoscaler VM {vm_name} in namespace {namespace}"
            if last_error is not None:
                raise VelaDeploymentError(message) from last_error
            raise VelaDeploymentError(message)

        await asyncio.sleep(_OVERLAY_IP_POLL_INTERVAL_SECONDS)


def _overlay_service_specs() -> list[tuple[str, int, str]]:
    return [
        (branch_service_name("db"), 5432, "postgres"),
        (branch_service_name("pgbouncer"), 6432, "pgbouncer"),
        (branch_service_name("rest"), 3000, "http"),
        (branch_service_name("storage"), 5000, "http"),
        (branch_service_name("meta"), 8080, "http"),
        (branch_service_name("pgexporter"), 9187, "http"),
    ]


async def _ensure_autoscaler_overlay_endpoint_slices(namespace: str, overlay_ip: str) -> None:
    await asyncio.gather(
        *(
            kube_service.ensure_endpoint_slice(
                namespace=namespace,
                slice_name=service_name,
                service_name=service_name,
                address=overlay_ip,
                port=port,
                port_name=port_name,
            )
            for service_name, port, port_name in _overlay_service_specs()
        )
    )


async def _initialize_autoscaler_overlay_endpoints(namespace: str) -> None:
    vm_name = _autoscaler_vm_name()
    overlay_ip = await _wait_for_autoscaler_overlay_ip(namespace, vm_name)
    await _ensure_autoscaler_overlay_endpoint_slices(namespace, overlay_ip)


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


async def load_simplyblock_credentials() -> tuple[str, str, str]:
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


async def _resolve_volume_identifiers(namespace: str, pvc_name: str) -> tuple[str, str | None]:
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


async def resolve_storage_volume_identifiers(namespace: str) -> tuple[str, str | None]:
    pvc_name = f"{_autoscaler_vm_name()}{STORAGE_PVC_SUFFIX}"
    return await _resolve_volume_identifiers(namespace, pvc_name)


async def resolve_autoscaler_volume_identifiers(namespace: str) -> tuple[str, str | None]:
    pvc_name = f"{_autoscaler_vm_name()}{AUTOSCALER_PVC_SUFFIX}"
    return await _resolve_volume_identifiers(namespace, pvc_name)


async def update_branch_volume_iops(branch_id: Identifier, iops: int) -> None:
    namespace = deployment_namespace(branch_id)

    volume_uuid, _ = await resolve_autoscaler_volume_identifiers(namespace)
    try:
        async with create_simplyblock_api() as sb_api:
            await sb_api.update_volume(volume_uuid=volume_uuid, payload={"max_rw_iops": iops})
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip() or exc.response.reason_phrase or str(exc)
        raise VelaDeploymentError(
            f"Simplyblock volume API rejected IOPS update for volume {volume_uuid!r}: {detail}"
        ) from exc
    except httpx.HTTPError as exc:
        raise VelaDeploymentError("Failed to reach Simplyblock volume API") from exc

    logger.info("Updated Simplyblock volume %s IOPS to %s", volume_uuid, iops)


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


def _load_compose_manifest() -> dict[str, Any]:
    compose_resource = resources.files(__package__).joinpath("compose.yml")
    compose_content = yaml.safe_load(compose_resource.read_text())
    if not isinstance(compose_content, dict):
        raise VelaDeploymentError("docker-compose manifest must be a mapping")
    return compose_content


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
        raise VelaDeploymentError("vela chart values.yaml must be a mapping")
    return values_content


def _configure_vela_values(
    values_content: dict[str, Any],
    *,
    parameters: DeploymentParameters,
    jwt_secret: str,
    pgbouncer_admin_password: str,
    storage_class_name: str,
    use_existing_db_pvc: bool,
    pgbouncer_config: Mapping[str, int] | None,
    enable_file_storage: bool,
) -> dict[str, Any]:
    pgbouncer_values = values_content.setdefault("pgbouncer", {})
    pgbouncer_cfg = pgbouncer_values.setdefault("config", {})
    if pgbouncer_config:
        for key in (
            "default_pool_size",
            "max_client_conn",
            "reserve_pool_size",
            "query_wait_timeout",
            "server_idle_timeout",
            "server_lifetime",
        ):
            value = pgbouncer_config.get(key)
            if value is not None:
                pgbouncer_cfg[key] = value

    db_spec = values_content.setdefault("db", {})
    db_service_cfg = db_spec.setdefault("service", {})
    db_service_cfg["externalEnabled"] = get_settings().enable_db_external_ipv6_loadbalancer

    secrets = values_content.setdefault("secret", {})
    secrets.setdefault("jwt", {}).update(
        secret=jwt_secret,
    )
    secrets.update(pgmeta_crypto_key=get_settings().pgmeta_crypto_key)
    secrets.setdefault("db", {})["password"] = parameters.database_password
    secrets.setdefault("pgbouncer", {})["admin_password"] = pgbouncer_admin_password

    db_values = values_content.setdefault("db", {})
    db_service_cfg = db_values.setdefault("service", {})
    db_service_cfg["externalEnabled"] = get_settings().enable_db_external_ipv6_loadbalancer

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

    autoscaler_spec = values_content.setdefault("autoscalerVm", {})
    autoscaler_spec["enabled"] = True

    image = database_image_tag_to_database_images(parameters.database_image_tag)
    autoscaler_image = autoscaler_spec.setdefault("image", {})
    autoscaler_image["repository"] = image["image"]
    autoscaler_image["tag"] = image["tag"]
    autoscaler_resources = autoscaler_spec.setdefault("resources", {})
    autoscaler_resources["cpus"] = calculate_autoscaler_vm_cpus(parameters.milli_vcpu)
    memory_slot_size, memory_slots = calculate_autoscaler_vm_memory(parameters.memory_bytes)
    autoscaler_resources["memorySlotSize"] = memory_slot_size
    autoscaler_resources["memorySlots"] = memory_slots

    autoscaler_persistence = autoscaler_spec.setdefault("persistence", {})
    autoscaler_persistence["create"] = not use_existing_db_pvc
    autoscaler_persistence["claimName"] = f"{_autoscaler_vm_name()}{AUTOSCALER_PVC_SUFFIX}"
    autoscaler_persistence["size"] = f"{bytes_to_gb(parameters.database_size)}G"
    autoscaler_persistence["storageClassName"] = storage_class_name
    autoscaler_persistence.setdefault("accessModes", ["ReadWriteMany"])

    return values_content


async def create_vela_config(
    branch_id: Identifier,
    parameters: DeploymentParameters,
    branch: Name,
    jwt_secret: str,
    pgbouncer_admin_password: str,
    *,
    use_existing_db_pvc: bool = False,
    ensure_namespace: bool = True,
    pgbouncer_config: Mapping[str, int] | None = None,
):
    namespace = deployment_namespace(branch_id)
    logging.info(
        "Creating Vela configuration for namespace: %s (branch %s, branch_id=%s)",
        namespace,
        branch,
        branch_id,
    )

    if ensure_namespace:
        await kube_service.ensure_namespace(namespace, labels=_POD_SECURITY_LABELS)

    chart = resources.files(__package__) / "charts" / "vela"
    compose_file = _configure_compose_storage(
        _load_compose_manifest(),
        enable_file_storage=parameters.enable_file_storage,
    )
    vector_resource = resources.files(__package__).joinpath("vector.yml")
    pb_hba_resource = resources.files(__package__).joinpath("pg_hba.conf")
    postgresql_resource = resources.files(__package__).joinpath("postgresql.conf")
    values_content = _load_chart_values(chart)

    storage_class_name = await ensure_branch_storage_class(branch_id, iops=parameters.iops)
    values_content = _configure_vela_values(
        values_content,
        parameters=parameters,
        jwt_secret=jwt_secret,
        pgbouncer_admin_password=pgbouncer_admin_password,
        storage_class_name=storage_class_name,
        use_existing_db_pvc=use_existing_db_pvc,
        pgbouncer_config=pgbouncer_config,
        enable_file_storage=parameters.enable_file_storage,
    )

    with (
        resources.as_file(vector_resource) as vector_file,
        resources.as_file(pb_hba_resource) as pb_hba_conf,
        resources.as_file(postgresql_resource) as postgresql_conf,
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
                    _release_name(),
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
                    "--set-file",
                    f"postgresqlConf={postgresql_conf}",
                    "-f",
                    temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
            await _initialize_autoscaler_overlay_endpoints(namespace)
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create deployment: {e.stderr}")
            release_name = _release_name()
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


async def _delete_autoscaler_vm(namespace: str) -> None:
    vm_name = _autoscaler_vm_name()
    async with custom_api_client() as custom_client:
        try:
            await custom_client.delete_namespaced_custom_object(
                group="vm.neon.tech",
                version="v1",
                namespace=namespace,
                plural="virtualmachines",
                name=vm_name,
                body=kubernetes_client.V1DeleteOptions(),
            )
            logger.info("Deleted autoscaler VM %s in namespace %s", vm_name, namespace)
        except ApiException as exc:
            if exc.status == 404:
                logger.info("Autoscaler VM %s not found in namespace %s; skipping delete", vm_name, namespace)
                return
            raise


async def delete_deployment(branch_id: Identifier) -> None:
    namespace, _ = get_autoscaler_vm_identity(branch_id)
    storage_class_name = branch_storage_class_name(branch_id)
    await cleanup_branch_dns(branch_id)
    await _delete_autoscaler_vm(namespace)
    try:
        await kube_service.delete_namespace(namespace)
    except ApiException as exc:
        if exc.status == 404:
            logger.info("Namespace %s not found", namespace)
        else:
            raise
    try:
        await delete_vela_grafana_obj(branch_id)
    except VelaGrafanaError:
        logger.info("Grafana dashboard for branch %s not found", branch_id)
    try:
        await kube_service.delete_storage_class(storage_class_name)
    except ApiException as exc:
        if exc.status == 404:
            logger.info("StorageClass %s not found", storage_class_name)
        else:
            raise


def get_autoscaler_vm_identity(branch_id: Identifier) -> tuple[str, str]:
    """
    Return the (namespace, vm_name) for the branch's autoscaler Neon VirtualMachine.
    """
    namespace = deployment_namespace(branch_id)
    vm_name = _autoscaler_vm_name()
    return namespace, vm_name


def calculate_cpu_resources(milli_vcpu: int) -> tuple[str, str]:
    """Return (limit, request) CPU quantities formatted for Kubernetes."""

    cpu_limit = f"{milli_vcpu}m"
    cpu_request_milli = max(1, int(milli_vcpu * CPU_REQUEST_FRACTION))
    cpu_request = f"{cpu_request_milli}m"
    return cpu_limit, cpu_request


def calculate_autoscaler_vm_cpus(milli_vcpu: int) -> dict[str, str]:
    """
    Return min/use/max CPU core counts for the autoscaler VM derived from milli vCPU.
    """

    vm_millis = max(1, milli_vcpu)
    cpu_value = f"{vm_millis}m"
    min_value = f"{VCPU_MILLIS_MIN}m"
    max_value = f"{VCPU_MILLIS_MAX}m"
    return {"min": min_value, "use": cpu_value, "max": max_value}


def calculate_autoscaler_vm_memory(memory_bytes: int) -> tuple[str, dict[str, int]]:
    """
    Return (memory_slot_size, memory_slots) derived from memory_bytes with fixed slot sizing.
    """

    memory_mib = max(1, bytes_to_mib(memory_bytes))
    slot_size_mib = AUTOSCALER_MEMORY_SLOT_SIZE_MIB
    desired_slots = max(1, math.ceil(memory_mib / slot_size_mib))
    target_memory_slots = min(max(desired_slots, AUTOSCALER_MEMORY_SLOTS_MIN), AUTOSCALER_MEMORY_SLOTS_MAX)
    slots = {
        "min": AUTOSCALER_MEMORY_SLOTS_MIN,
        "use": target_memory_slots,
        "limit": target_memory_slots,
        "max": AUTOSCALER_MEMORY_SLOTS_MAX,
    }
    return f"{slot_size_mib}Mi", slots


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
    host: str = f"{branch_service_name('db')}.{namespace}.svc.cluster.local"
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


class KubeGatewayConfig(BaseModel):
    namespace: str = ""
    gateway_name: str = Field(default_factory=lambda: get_settings().gateway_name)
    gateway_namespace: str = Field(default_factory=lambda: get_settings().gateway_namespace)

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


async def _create_dns_record(
    cf: CloudflareSettings,
    *,
    domain: str,
    record_type: DNSRecordType,
    content: str,
    proxied: bool,
    ttl: int = 1,
) -> None:
    try:
        async with AsyncCloudflare(api_token=cf.api_token) as client:
            await client.dns.records.create(
                zone_id=cf.zone_id,
                name=domain,
                type=record_type,
                content=content,
                ttl=ttl,
                proxied=proxied,
            )
    except CloudflareError as exc:
        raise VelaCloudflareError(f"Cloudflare API error: {exc}") from exc
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaCloudflareError(f"Cloudflare request failed: {exc}") from exc

    logger.info("Created DNS %s record %s -> %s", record_type, domain, content)


async def _delete_dns_records(cf: CloudflareSettings, *, domain: str, record_type: DNSRecordType) -> None:
    """
    Delete all Cloudflare DNS records matching the given domain and type.

    Args:
        cf: Cloudflare configuration
        domain: The fully qualified domain name to delete records for
        record_type: The DNS record type (AAAA or CNAME)

    Raises:
        VelaCloudflareError: If the Cloudflare API request fails
    """
    try:
        async with AsyncCloudflare(api_token=cf.api_token) as client:
            records = await client.dns.records.list(
                zone_id=cf.zone_id,
                name=cast("CloudflareRecordName", domain),
                type=record_type,
            )
            if not records:
                logger.info("No Cloudflare DNS %s records found for %s", record_type, domain)
                return
            for record in records:
                record_id = getattr(record, "id", None)
                if not record_id:
                    logger.warning("Skipping Cloudflare DNS record for %s with missing id", domain)
                    continue
                await client.dns.records.delete(zone_id=cf.zone_id, dns_record_id=record_id)
                logger.info(
                    "Deleted Cloudflare DNS %s record %s (id=%s)",
                    record_type,
                    domain,
                    record_id,
                )
    except CloudflareError as exc:
        raise VelaCloudflareError(f"Cloudflare API error while deleting DNS record for {domain!r}: {exc}") from exc
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaCloudflareError(f"Cloudflare request failed while deleting DNS record for {domain!r}: {exc}") from exc


async def cleanup_branch_dns(branch_id: Identifier) -> None:
    """
    Delete Cloudflare DNS records for a branch.

    Removes both the API CNAME record and database AAAA record for the given branch.
    Record deletions themselves log warnings instead of raising, but errors resolving
    Cloudflare configuration or branch domains still propagate to the caller.

    Args:
        branch_id: The branch identifier to clean up DNS records for
    """
    cf_cfg = get_settings().cloudflare
    deletions = []
    record_types = []

    api_domain = branch_api_domain(branch_id)
    if api_domain:
        deletions.append(_delete_dns_records(cf_cfg, domain=api_domain, record_type="CNAME"))
        record_types.append(f"API CNAME ({api_domain})")

    db_domain = branch_db_domain(branch_id)
    if db_domain:
        deletions.append(
            _delete_dns_records(
                cf_cfg,
                domain=db_domain,
                record_type=DATABASE_DNS_RECORD_TYPE,
            )
        )
        record_types.append(f"database CNAME ({db_domain})")

    if not deletions:
        logger.info(
            "Skipping Cloudflare DNS cleanup for branch %s because domain suffix is not configured",
            branch_id,
        )
        return

    results = await asyncio.gather(*deletions, return_exceptions=True)
    for record_type, result in zip(record_types, results, strict=True):
        if isinstance(result, Exception):
            logger.error("DNS deletion failed for %s: %s", record_type, result)


def _get_value(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict):
            value = obj.get(name)
            if value is not None:
                return value
        value = getattr(obj, name, None)
        if value is not None:
            return value
    return None


def _extract_load_balancer_ipv6(service: Any) -> str | None:
    status = _get_value(service, "status")
    if not status:
        return None

    load_balancer = _get_value(status, "load_balancer", "loadBalancer")
    if not load_balancer:
        return None

    ingress = _get_value(load_balancer, "ingress")
    if not ingress:
        return None

    for entry in ingress:
        ip_address = _get_value(entry, "ip")
        if ip_address and ":" in ip_address:
            return ip_address
    return None


async def _wait_for_service_ipv6(namespace: str, service_name: str) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _LOAD_BALANCER_TIMEOUT_SECONDS
    last_error: Exception | None = None

    while True:
        try:
            service = await kube_service.get_service(namespace, service_name)
        except VelaKubernetesError as exc:
            last_error = exc
            service = None
        else:
            last_error = None

        if service:
            ipv6_address = _extract_load_balancer_ipv6(service)
            if ipv6_address:
                logger.info(
                    "Service %s/%s assigned IPv6 LoadBalancer address %s",
                    namespace,
                    service_name,
                    ipv6_address,
                )
                return ipv6_address

        if loop.time() >= deadline:
            timeout_msg = f"Timed out waiting for IPv6 LoadBalancer address on service {namespace!r}/{service_name!r}"
            if last_error is not None:
                raise VelaKubernetesError(timeout_msg) from last_error
            raise VelaKubernetesError(timeout_msg)

        await asyncio.sleep(_LOAD_BALANCER_POLL_INTERVAL_SECONDS)


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
            service_name=branch_service_name("rest"),
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
            service_name=branch_service_name("storage"),
            service_port=5000,
            path_prefix="/storage",
            route_suffix="storage-route",
            plugins=["cors", APIKEY_JWT_PLUGIN_NAME],
        ),
    ]


def _pgmeta_route_specs(ref: str, domain: str, namespace: str) -> list[HTTPRouteSpec]:
    """HTTPRoute definitions that expose the Postgres Meta service for a branch."""

    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name=branch_service_name("meta"),
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


def _build_kong_plugins(namespace: str, jwt_secret: str) -> list[dict[str, Any]]:
    return [
        _build_cors_plugin(namespace),
        _build_check_encrypted_header_plugin(namespace),
        _build_apikey_jwt_plugin(namespace, jwt_secret),
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


def _build_apikey_jwt_plugin(namespace: str, jwt_secret: str) -> dict[str, Any]:
    if not jwt_secret:
        raise VelaDeploymentError("JWT secret is required for Kong pre-function plugin")

    jwt_secret_literal = json.dumps(jwt_secret)
    lua_script = textwrap.dedent(
        f"""
        local method = kong.request.get_method()
        if method == "OPTIONS" then
            return
        end

        -- Allow public paths
        local public_path_prefix = "/storage/object/public"
        local path = kong.request.get_path()
        if path and path:sub(1, public_path_prefix:len()) == public_path_prefix then
            return
        end

        local auth_header = kong.request.get_header("Authorization")
        if not auth_header then
            return kong.response.exit(403, {{ message = "Missing Authorization header" }})
        end

        -- Expect: Bearer <token>
        local token = auth_header:match("^Bearer%s+(.+)$")
        if not token then
            return kong.response.exit(401, {{ message = "Invalid Authorization header format" }})
        end

        -- Verify JWT: (verifies Signature and its validity)
        local jwt = require "resty.jwt"
        local jwt_secret = {jwt_secret_literal}
        local jwt_obj = jwt:verify(jwt_secret, token)

        if not jwt_obj.verified then
            return kong.response.exit(401, {{
                message = "Invalid token",
                reason = jwt_obj.reason
            }})
        end

        return
        """
    ).strip()

    return {
        "apiVersion": "configuration.konghq.com/v1",
        "kind": "KongPlugin",
        "metadata": {
            "name": APIKEY_JWT_PLUGIN_NAME,
            "namespace": namespace,
        },
        "plugin": "pre-function",
        "config": {
            "access": [lua_script],
        },
    }


async def _apply_kong_plugin(namespace: str, plugin: dict[str, Any]) -> None:
    """Apply KongPlugin manifest without blocking the event loop."""
    try:
        await kube_service.apply_kong_plugin(namespace, plugin)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise VelaKubernetesError(f"Failed to apply KongPlugin: {exc}") from exc


async def provision_branch_endpoints(
    spec: BranchEndpointProvisionSpec,
    *,
    ref: str,
) -> BranchEndpointResult:
    """Provision DNS + HTTPRoute resources (PostgREST + optional Storage + PGMeta) for a branch."""

    cf_cfg = get_settings().cloudflare

    gateway_cfg = KubeGatewayConfig().for_namespace(deployment_namespace(spec.branch_id))

    domain = f"{ref}.{cf_cfg.domain_suffix}".lower()
    logger.info(
        "Provisioning endpoints for project_id=%s branch_id=%s branch_slug=%s domain=%s",
        spec.project_id,
        spec.branch_id,
        spec.branch_slug,
        domain,
    )

    # Apply the KongPlugins required for the branch routes
    kong_plugins = _build_kong_plugins(gateway_cfg.namespace, spec.jwt_secret)
    await asyncio.gather(*(_apply_kong_plugin(gateway_cfg.namespace, plugin) for plugin in kong_plugins))

    route_specs = _postgrest_route_specs(ref, domain, gateway_cfg.namespace)
    if spec.enable_file_storage:
        route_specs += _storage_route_specs(ref, domain, gateway_cfg.namespace)
    route_specs += _pgmeta_route_specs(ref, domain, gateway_cfg.namespace)
    routes = [_build_http_route(gateway_cfg, route_spec) for route_spec in route_specs]
    await _apply_http_routes(gateway_cfg.namespace, routes)

    db_domain = branch_db_domain(spec.branch_id)
    domain_dns_task = asyncio.create_task(
        _create_dns_record(
            cf_cfg,
            domain=domain,
            record_type="CNAME",
            content=cf_cfg.branch_ref,
            proxied=False,
        )
    )
    db_domain_dns_task = asyncio.create_task(
        _create_dns_record(
            cf_cfg,
            domain=db_domain,
            record_type=DATABASE_DNS_RECORD_TYPE,
            content=cf_cfg.branch_db_ref,
            proxied=False,
        )
    )
    await asyncio.gather(domain_dns_task, db_domain_dns_task)

    return BranchEndpointResult(ref=ref, domain=domain)


async def deploy_branch_environment(
    *,
    organization_id: Identifier,
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: Name,
    credential: str,
    parameters: DeploymentParameters,
    jwt_secret: str,
    pgbouncer_admin_password: str,
    pgbouncer_config: Mapping[str, int],
    use_existing_pvc: bool = False,
) -> None:
    """Background task: provision infra for a branch and persist the resulting endpoint."""
    await kube_service.ensure_namespace(deployment_namespace(branch_id), labels=_POD_SECURITY_LABELS)
    ref = branch_dns_label(branch_id)

    results = await asyncio.gather(
        create_vela_config(
            branch_id=branch_id,
            parameters=parameters,
            branch=branch_slug,
            jwt_secret=jwt_secret,
            pgbouncer_admin_password=pgbouncer_admin_password,
            use_existing_db_pvc=use_existing_pvc,
            ensure_namespace=False,
            pgbouncer_config=pgbouncer_config,
        ),
        provision_branch_endpoints(
            spec=BranchEndpointProvisionSpec(
                project_id=project_id,
                branch_id=branch_id,
                branch_slug=branch_slug,
                enable_file_storage=parameters.enable_file_storage,
                jwt_secret=jwt_secret,
            ),
            ref=ref,
        ),
        create_vela_grafana_obj(organization_id, branch_id, credential),
        return_exceptions=True,
    )

    if exceptions := [result for result in results if isinstance(result, Exception)]:
        raise VelaDeployError("Failed operations during vela deployment", exceptions)
