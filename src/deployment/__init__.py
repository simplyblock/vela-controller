import asyncio
import logging
import subprocess
import tempfile
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from importlib import resources
from typing import Annotated, Any, Literal

import yaml
from cloudflare import AsyncCloudflare, CloudflareError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import BaseModel, Field, field_validator
from urllib3.exceptions import HTTPError

from .. import VelaError
from .._util import GIB, Identifier, Slug, bytes_to_gib, check_output, dbstr
from .kubernetes import KubernetesService
from .kubevirt import patch_virtual_machine
from .rebalance import rebalance_virtual_machines
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()

DEFAULT_GATEWAY_NAME = "public-gateway"
DEFAULT_GATEWAY_NAMESPACE = "kong-system"

CPU_INCREMENT = Decimal("0.1")
CPU_MIN = Decimal("0.1")
CPU_MAX = Decimal("64")
CPU_REQUEST_FACTOR = Decimal("0.25")

MEMORY_INCREMENT = Decimal("0.1")
MEMORY_MIN = Decimal("0.1")
MEMORY_MAX = Decimal("256")
MEMORY_REQUEST_FACTOR = Decimal("0.9")


def _format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _ensure_step(value: Decimal, *, increment: Decimal, field_name: str) -> Decimal:
    scaled = value / increment
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{field_name} must be a multiple of {increment}")
    return value


def _ceil_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _cpu_quantity(cpu: Decimal) -> str:
    if cpu <= 0:
        raise ValueError("CPU quantity must be positive")
    if cpu == cpu.to_integral_value():
        return _format_decimal(cpu)
    millicpu = int((cpu * Decimal("1000")).to_integral_value(rounding=ROUND_HALF_UP))
    return f"{millicpu}m"


def _memory_quantity(memory_gib: Decimal) -> str:
    if memory_gib <= 0:
        raise ValueError("Memory quantity must be positive")
    return f"{_format_decimal(memory_gib)}Gi"


def _cpu_topology(cpu: Decimal) -> dict[str, int]:
    total_vcpus = max(1, min(_ceil_decimal(cpu), int(CPU_MAX)))
    sockets = 1 if total_vcpus <= 32 else 2
    cores_per_socket = max(1, min(32, (total_vcpus + sockets - 1) // sockets))
    return {
        "sockets": sockets,
        "cores": cores_per_socket,
        "threads": 1,
        "maxSockets": 2,
        "maxCores": 32,
        "maxThreads": 1,
    }


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
    vcpu: Annotated[Decimal, Field(ge=CPU_MIN, le=CPU_MAX)]
    memory: Annotated[Decimal, Field(ge=MEMORY_MIN, le=MEMORY_MAX)]  # Expressed in GiB
    iops: Annotated[int, Field(gt=0, le=2**31 - 1)]
    database_image_tag: Literal["15.1.0.147"]

    @field_validator("vcpu", "memory", mode="before")
    @classmethod
    def _coerce_decimal(cls, value):
        if isinstance(value, float):
            return Decimal(str(value))
        return value

    @field_validator("vcpu", mode="after")
    @classmethod
    def _validate_vcpu(cls, value: Decimal) -> Decimal:
        value = _ensure_step(value, increment=CPU_INCREMENT, field_name="vcpu").quantize(CPU_INCREMENT)
        if value < CPU_MIN:
            raise ValueError(f"vcpu must be at least {CPU_MIN}")
        return value

    @field_validator("memory", mode="after")
    @classmethod
    def _validate_memory(cls, value: Decimal) -> Decimal:
        value = _ensure_step(value, increment=MEMORY_INCREMENT, field_name="memory").quantize(MEMORY_INCREMENT)
        if value < MEMORY_MIN:
            raise ValueError(f"memory must be at least {MEMORY_MIN} GiB")
        return value


StatusType = Literal["ACTIVE_HEALTHY", "ACTIVE_UNHEALTHY", "COMING_UP", "INACTIVE", "UNKNOWN"]


class DeploymentStatus(BaseModel):
    status: StatusType
    pods: dict[str, str]
    message: str


def _build_vm_resource_patch(cpu: Decimal | None, memory: Decimal | None) -> dict[str, Any]:
    spec: dict[str, Any] = {}
    template: dict[str, Any] = {}
    template_spec: dict[str, Any] = {}
    domain: dict[str, Any] = {}
    resources: dict[str, Any] = {}
    requests: dict[str, Any] = {}
    limits: dict[str, Any] = {}

    if cpu is not None:
        cpu_request_value = (cpu * CPU_REQUEST_FACTOR).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        domain["cpu"] = _cpu_topology(cpu)
        requests["cpu"] = _cpu_quantity(cpu_request_value)
        limits["cpu"] = _cpu_quantity(cpu)

    if memory is not None:
        memory_request_value = (memory * MEMORY_REQUEST_FACTOR).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        domain["memory"] = {
            "guest": _memory_quantity(memory),
            "maxGuest": _memory_quantity(MEMORY_MAX),
        }
        requests["memory"] = _memory_quantity(memory_request_value)
        limits["memory"] = _memory_quantity(memory)

    if requests:
        resources["requests"] = requests
    if limits:
        resources["limits"] = limits
    if resources:
        domain["resources"] = resources
    if domain:
        template_spec["domain"] = domain
    if template_spec:
        template["spec"] = template_spec
    if template:
        spec["template"] = template
    if spec:
        return {"spec": spec}
    return {}


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
    cpu_spec = db_spec.setdefault("cpu", {})
    cpu_request_value = (parameters.vcpu * CPU_REQUEST_FACTOR).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    cpu_spec["limit"] = _cpu_quantity(parameters.vcpu)
    cpu_spec["request"] = _cpu_quantity(cpu_request_value)
    cpu_spec["topology"] = _cpu_topology(parameters.vcpu)

    memory_spec = db_spec.setdefault("memory", {})
    memory_limit_value = parameters.memory
    memory_request_value = (parameters.memory * MEMORY_REQUEST_FACTOR).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    memory_spec["limit"] = _memory_quantity(memory_limit_value)
    memory_spec["request"] = _memory_quantity(memory_request_value)
    memory_spec["guest"] = _memory_quantity(memory_limit_value)
    memory_spec["maxGuest"] = _memory_quantity(MEMORY_MAX)

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


async def get_deployment_status(id_: Identifier, branch: Slug) -> DeploymentStatus:
    status: StatusType

    try:
        k8s_statuses = await kube_service.check_namespace_status(deployment_namespace(id_, branch))

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

    return DeploymentStatus(status=status, pods=k8s_statuses, message=message)


async def delete_deployment(id_: Identifier, branch: Slug) -> None:
    namespace = deployment_namespace(id_, branch)
    await asyncio.to_thread(
        subprocess.check_call,
        ["helm", "uninstall", _release_name(namespace), "-n", namespace, "--wait"],
    )
    await kube_service.delete_namespace(namespace)


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


async def apply_vm_runtime_resources(
    id_: Identifier,
    branch: Slug,
    *,
    cpu: Decimal | None,
    memory: Decimal | None,
) -> None:
    patch = _build_vm_resource_patch(cpu, memory)
    if not patch:
        return

    namespace, vm_name = get_db_vmi_identity(id_, branch)
    logger.info(
        "Applying VM resource patch for %s/%s (cpu=%s, memory=%s)",
        namespace,
        vm_name,
        cpu,
        memory,
    )
    await patch_virtual_machine(namespace, vm_name, patch)
    await rebalance_virtual_machines()


class ResizeParameters(BaseModel):
    database_size: Annotated[int | None, Field(gt=0, multiple_of=GIB)] = None
    vcpu: Annotated[Decimal | None, Field(ge=CPU_MIN, le=CPU_MAX)] = None
    memory: Annotated[Decimal | None, Field(ge=MEMORY_MIN, le=MEMORY_MAX)] = None  # GiB

    @field_validator("vcpu", "memory", mode="before")
    @classmethod
    def _coerce_decimal(cls, value):
        if value is None:
            return value
        if isinstance(value, float):
            return Decimal(str(value))
        return value

    @field_validator("vcpu", mode="after")
    @classmethod
    def _validate_resize_vcpu(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return value
        return _ensure_step(value, increment=CPU_INCREMENT, field_name="vcpu").quantize(CPU_INCREMENT)

    @field_validator("memory", mode="after")
    @classmethod
    def _validate_resize_memory(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return value
        return _ensure_step(value, increment=MEMORY_INCREMENT, field_name="memory").quantize(MEMORY_INCREMENT)


def resize_deployment(id_: Identifier, name: str, parameters: ResizeParameters):
    """Perform an in-place Helm upgrade to disk. Only parameters provided will be updated.
    others are preserved using --reuse-values.
    """
    chart = resources.files(__package__) / "charts" / "supabase"
    # Minimal values file with only overrides
    values_content: dict = {}
    db_spec = values_content.setdefault("db", {})
    if parameters.database_size is not None:
        db_spec.setdefault("persistence", {})["size"] = f"{bytes_to_gib(parameters.database_size)}Gi"

    namespace = deployment_namespace(id_, name)
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
        await kube_service.apply_http_routes(namespace, routes)
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

    await rebalance_virtual_machines()
