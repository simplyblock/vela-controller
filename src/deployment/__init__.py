import asyncio
import logging
import subprocess
import tempfile
from collections.abc import Mapping
from importlib import resources
from typing import Annotated, Any, Literal

import yaml
from cloudflare import AsyncCloudflare, CloudflareError
from kubernetes.client.rest import ApiException
from kubernetes.utils.quantity import parse_quantity
from pydantic import BaseModel, Field
from urllib3.exceptions import HTTPError

from .. import VelaError
from .._util import GIB, Identifier, Slug, bytes_to_gib, check_output, dbstr
from .kubernetes import KubernetesService
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()

DEFAULT_GATEWAY_NAME = "public-gateway"
DEFAULT_GATEWAY_NAMESPACE = "kong-system"
MIN_MEMORY_BYTES = GIB


def _default_branch_slug() -> Slug:
    from ..api.models.branch import Branch  # Local import to avoid circular dependency

    return Branch.DEFAULT_SLUG


def deployment_namespace(id_: Identifier, branch: Slug) -> str:
    branch = branch or _default_branch_slug()
    return f"{settings.deployment_namespace_prefix}-deployment-{id_}-{branch}"


def branch_dns_label(branch_id: Identifier) -> str:
    """Return the deterministic DNS label for a branch based on its ULID."""

    return str(branch_id).lower()


def branch_domain(branch_id: Identifier) -> str | None:
    """Compute the fully-qualified domain name for a branch."""

    suffix = settings.cloudflare_domain_suffix.strip()
    if not suffix:
        return None
    return f"{branch_dns_label(branch_id)}.{suffix}".lower()


def branch_rest_endpoint(branch_id: Identifier) -> str | None:
    """Return the PostgREST endpoint URL for a branch, if domain settings are available."""

    domain = branch_domain(branch_id)
    if not domain:
        return None
    return f"https://{domain}/rest"


def _release_name(namespace: str) -> str:
    return f"supabase-{namespace}"


class DeploymentParameters(BaseModel):
    database: dbstr
    database_user: dbstr
    database_password: dbstr
    database_size: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=GIB)]
    vcpu: Annotated[int, Field(gt=0, le=2**31 - 1)]
    memory: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=GIB)]
    iops: Annotated[int, Field(gt=0, le=2**31 - 1)]
    database_image_tag: Literal["15.1.0.147"]


StatusType = Literal["ACTIVE_HEALTHY", "ACTIVE_UNHEALTHY", "COMING_UP", "INACTIVE", "UNKNOWN"]


class DeploymentStatus(BaseModel):
    status: StatusType
    pods: dict[str, str]
    message: str


async def create_vela_config(id_: Identifier, parameters: DeploymentParameters, branch: Slug):
    logging.info(
        f"Creating Vela configuration for namespace: {deployment_namespace(id_, branch)}"
        f" (database {parameters.database}, user {parameters.database_user}, branch {branch})"
    )

    chart = resources.files(__package__) / "charts" / "supabase"
    values_content = yaml.safe_load((chart / "values.yaml").read_text())

    # Override defaults
    db_secrets = values_content.setdefault("db", {}).setdefault("credentials", {})
    db_secrets["adminusername"] = parameters.database_user
    db_secrets["adminpassword"] = parameters.database_password
    db_secrets["admindb"] = parameters.database

    db_spec = values_content.setdefault("db", {})
    db_spec["vcpu"] = parameters.vcpu
    db_spec["ram"] = bytes_to_gib(parameters.memory)
    db_spec.setdefault("persistence", {})["size"] = f"{bytes_to_gib(parameters.database_size)}Gi"
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag
    namespace = deployment_namespace(id_, branch)

    # todo: create an storage class with the given IOPS
    values_content["provisioning"] = {"iops": parameters.iops}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_values:
        yaml.safe_dump(values_content, temp_values, default_flow_style=False)

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
                    "-f",
                    temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create deployment: {e.stderr}")
            await check_output(
                ["helm", "uninstall", f"supabase-{namespace}", "-n", namespace],
                stderr=subprocess.PIPE,
                text=True,
            )
            raise


def _pods_with_status(statuses: dict[str, str], target_status: str) -> set[str]:
    return {name for name, status in statuses.items() if status == target_status}


def get_deployment_status(id_: Identifier, branch: Slug) -> DeploymentStatus:
    status: StatusType

    try:
        k8s_statuses = kube_service.check_namespace_status(deployment_namespace(id_, branch))

        if failed := _pods_with_status(k8s_statuses, "Failed"):
            status = "ACTIVE_UNHEALTHY"
            message = "Deployment has failed pods: " + ", ".join(failed)
        elif pending := _pods_with_status(k8s_statuses, "Pending"):
            status = "COMING_UP"
            message = "Deployment has pending pods: " + ", ".join(pending)
        elif succeeded := _pods_with_status(k8s_statuses, "Succeeded"):
            # succeeded implies a container is stopped, they should be running
            status = "INACTIVE"
            message = "Deployment has stopped pods: " + ", ".join(succeeded)
        elif all(status == "Running" for status in k8s_statuses.values()):
            status = "ACTIVE_HEALTHY"
            message = "All good :)"
        else:
            raise RuntimeError(
                "Unexpected status reported by kubernetes: "
                + "\n".join(f"{key}: {value}" for key, value in k8s_statuses.items())
            )

    except (ApiException, HTTPError, KeyError) as e:
        k8s_statuses = {}
        status = "UNKNOWN"
        message = str(e)

    return DeploymentStatus(
        status=status,
        pods=k8s_statuses,
        message=message,
    )


def delete_deployment(id_: Identifier, branch: Slug):
    namespace = deployment_namespace(id_, branch)
    subprocess.check_call(["helm", "uninstall", _release_name(namespace), "-n", namespace, "--wait"])
    kube_service.delete_namespace(namespace)


def get_db_vmi_identity(id_: Identifier, branch: Slug) -> tuple[str, str]:
    """
    Return the (namespace, vmi_name) for the project's database VirtualMachineInstance.

    The Helm chart defines the DB VM fullname as "{Release.Name}-{ChartName}-db" when no overrides
    are provided. Our release name is "supabase-{namespace}" and chart name is "supabase".
    Hence the VMI name resolves to: f"{_release_name(namespace)}-supabase-db".
    """
    namespace = deployment_namespace(id_, branch)
    vmi_name = f"{_release_name(namespace)}-supabase-db"
    return namespace, vmi_name


class ResizeParameters(BaseModel):
    database_size: Annotated[int | None, Field(gt=0, multiple_of=GIB)] = None
    memory: Annotated[int | None, Field(gt=0, multiple_of=GIB)] = None


def _nested_get(mapping: Any, *keys: str) -> Any:
    current = mapping
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _format_gib(quantity_bytes: int) -> str:
    return f"{bytes_to_gib(quantity_bytes)}Gi"


def _validate_cluster_hotplug_settings(requested_bytes: int) -> None:
    try:
        kubevirt_cfg = kube_service.get_kubevirt_config()
    except ApiException as exc:
        raise VelaError(f"Failed to read KubeVirt configuration: {exc}") from exc

    spec = kubevirt_cfg.get("spec", {}) if isinstance(kubevirt_cfg, Mapping) else {}

    methods = _nested_get(spec, "workloadUpdateStrategy", "workloadUpdateMethods") or []
    if "LiveMigrate" not in methods:
        raise VelaError("KubeVirt workloadUpdateStrategy must include LiveMigrate for memory hotplug")

    vm_rollout = _nested_get(spec, "configuration", "vmRolloutStrategy")
    if vm_rollout != "LiveUpdate":
        raise VelaError("KubeVirt vmRolloutStrategy must be LiveUpdate for memory hotplug to take effect")

    max_guest = _nested_get(spec, "configuration", "liveUpdateConfiguration", "maxGuest")
    if max_guest is not None:
        try:
            max_guest_bytes = parse_quantity(max_guest)
        except ValueError as exc:
            raise VelaError(f"Unable to parse liveUpdateConfiguration.maxGuest value {max_guest!r}") from exc
        if requested_bytes > max_guest_bytes:
            raise VelaError(f"Requested memory {_format_gib(requested_bytes)} exceeds cluster maxGuest {max_guest}")


def _get_vm_guest_bytes(namespace: str, vm_name: str) -> int:
    try:
        vm = kube_service.get_virtual_machine(namespace, vm_name)
    except ApiException as exc:
        raise VelaError(f"VirtualMachine {vm_name} not found in namespace {namespace}: {exc}") from exc

    vm_guest_quantity = _nested_get(vm, "spec", "template", "spec", "domain", "memory", "guest")
    if not vm_guest_quantity:
        raise VelaError("VirtualMachine is missing spec.template.spec.domain.memory.guest")

    try:
        current_vm_guest_bytes = parse_quantity(vm_guest_quantity)
    except ValueError as exc:
        raise VelaError(f"Unable to parse VM guest memory value {vm_guest_quantity!r}") from exc

    return current_vm_guest_bytes


def _get_initial_memory_status(namespace: str, vm_name: str) -> dict[str, str]:
    try:
        memory_status = kube_service.get_vmi_memory_status(namespace, vm_name)
    except ApiException as exc:
        raise VelaError(f"Failed to fetch VirtualMachineInstance {vm_name}: {exc}") from exc

    if not memory_status:
        raise VelaError(
            "VirtualMachineInstance status.memory is unavailable; ensure the workload is running before resizing"
        )

    return memory_status


def _ensure_memory_hotplug_preconditions(
    namespace: str,
    vm_name: str,
    requested_bytes: int,
) -> dict[str, str]:
    if requested_bytes < MIN_MEMORY_BYTES:
        raise VelaError("Memory hotplug requires at least 1Gi of guest memory")

    _validate_cluster_hotplug_settings(requested_bytes)

    current_vm_guest_bytes = _get_vm_guest_bytes(namespace, vm_name)

    if current_vm_guest_bytes < MIN_MEMORY_BYTES:
        raise VelaError("VirtualMachine must be provisioned with at least 1Gi to allow hotplug")

    return _get_initial_memory_status(namespace, vm_name)


def resize_deployment(id_: Identifier, name: str, parameters: ResizeParameters):
    """Perform an in-place Helm upgrade to disk. Only parameters provided will be updated.
    others are preserved using --reuse-values.
    """
    chart = resources.files(__package__) / "charts" / "supabase"
    # Minimal values file with only overrides
    values_content: dict = {}
    db_spec = values_content.setdefault("db", {})
    memory_status_before: dict[str, str] | None = None
    target_memory_quantity: str | None = None

    if parameters.database_size is not None:
        db_spec.setdefault("persistence", {})["size"] = f"{bytes_to_gib(parameters.database_size)}Gi"

    namespace = deployment_namespace(id_, name)
    vm_namespace, vm_name = get_db_vmi_identity(id_, name)

    if parameters.memory is not None:
        memory_status_before = _ensure_memory_hotplug_preconditions(vm_namespace, vm_name, parameters.memory)
        target_memory_quantity = _format_gib(parameters.memory)
        db_spec["ram"] = bytes_to_gib(parameters.memory)
        logger.info(
            "Existing memory status for %s/%s: %s",
            vm_namespace,
            vm_name,
            memory_status_before,
        )

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

    if target_memory_quantity:
        try:
            memory_status_after = kube_service.wait_for_vmi_guest_requested(
                vm_namespace,
                vm_name,
                target_memory_quantity,
            )
        except (ApiException, RuntimeError) as exc:
            raise VelaError(f"Memory hotplug did not complete successfully: {exc}") from exc

        logger.info(
            "Updated memory status for %s/%s: %s",
            vm_namespace,
            vm_name,
            memory_status_after,
        )


class BranchEndpointError(VelaError):
    """Raised when provisioning branch endpoints fails."""


class CloudflareConfig(BaseModel):
    api_token: str
    zone_id: str
    branch_ref_cname: str
    domain_suffix: str


class KubeGatewayConfig(BaseModel):
    namespace: str = ""
    gateway_name: str = DEFAULT_GATEWAY_NAME
    gateway_namespace: str = DEFAULT_GATEWAY_NAMESPACE

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


class BranchEndpointProvisionSpec(BaseModel):
    project_id: Identifier
    branch_slug: str


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
        raise BranchEndpointError(f"Cloudflare API error: {exc}") from exc
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise BranchEndpointError(f"Cloudflare request failed: {exc}") from exc

    logger.info("Created DNS CNAME record %s -> %s", domain, cf.branch_ref_cname)


def _build_http_route(cfg: KubeGatewayConfig, spec: HTTPRouteSpec) -> dict[str, Any]:
    return {
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRoute",
        "metadata": {
            "name": f"{spec.ref}-{spec.route_suffix}",
            "namespace": spec.namespace,
            "annotations": {
                "konghq.com/strip-path": "true",
            },
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

    base_service = f"supabase-{namespace}-supabase"
    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name=f"{base_service}-rest",
            service_port=3000,
            path_prefix="/rest",
            route_suffix="postgrest-route",
        ),
    ]


async def _apply_http_routes(namespace: str, routes: list[dict[str, Any]]) -> None:
    """Apply HTTPRoute manifests without blocking the event loop."""
    try:
        await asyncio.to_thread(kube_service.apply_http_routes, namespace, routes)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise BranchEndpointError(f"Failed to apply HTTPRoute: {exc}") from exc


async def provision_branch_endpoints(
    spec: BranchEndpointProvisionSpec,
    *,
    ref: str,
) -> BranchEndpointResult:
    """Provision DNS + HTTPRoute resources (currently PostgREST only) for a branch."""

    cf_cfg = CloudflareConfig(
        api_token=settings.cloudflare_api_token,
        zone_id=settings.cloudflare_zone_id,
        branch_ref_cname=settings.cloudflare_branch_ref_cname,
        domain_suffix=settings.cloudflare_domain_suffix,
    )

    gateway_cfg = KubeGatewayConfig().for_namespace(deployment_namespace(spec.project_id, spec.branch_slug))

    domain = f"{ref}.{cf_cfg.domain_suffix}".lower()
    logger.info(
        "Provisioning endpoints for project_id=%s branch=%s domain=%s",
        spec.project_id,
        spec.branch_slug,
        domain,
    )

    await _create_dns_record(cf_cfg, domain)

    # Right now we expose only the PostgREST service; extend here when other components need routes.
    route_specs = _postgrest_route_specs(ref, domain, gateway_cfg.namespace)
    routes = [_build_http_route(gateway_cfg, route_spec) for route_spec in route_specs]
    await _apply_http_routes(gateway_cfg.namespace, routes)

    return BranchEndpointResult(ref=ref, domain=domain)


async def deploy_branch_environment(
    *,
    project_id: Identifier,
    branch_id: Identifier,
    branch_slug: Slug,
    parameters: DeploymentParameters,
) -> None:
    """Background task: provision infra for a branch and persist the resulting endpoint."""

    # Create the main deployment (database etc)
    await create_vela_config(project_id, parameters, branch_slug)

    # Provision DNS + HTTPRoute resources
    ref = branch_dns_label(branch_id)
    await provision_branch_endpoints(
        spec=BranchEndpointProvisionSpec(project_id=project_id, branch_slug=branch_slug), ref=ref
    )
