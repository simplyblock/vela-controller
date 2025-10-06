import asyncio
import logging
import subprocess
import tempfile
from importlib import resources
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from cloudflare import AsyncCloudflare, CloudflareError
from pydantic import BaseModel, Field

from .. import VelaError
from .._util import GIB, Identifier, Slug, StatusType, bytes_to_gib, check_output, dbstr
from .kubernetes import KubernetesService
from .kubevirt import get_virtualmachine_status
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()

DEFAULT_GATEWAY_NAME = "vela-public-gateway"
DEFAULT_GATEWAY_NAMESPACE = "kong-system"
DEFAULT_DATABASE_VM_NAME = "supabase-supabase-db"


def deployment_namespace(branch_id: Identifier) -> str:
    """Return the Kubernetes namespace for a branch using `<prefix>-<branch_id>` format."""

    branch_value = str(branch_id).lower()
    prefix = settings.deployment_namespace_prefix
    if prefix:
        return f"{prefix}-{branch_value}"
    return branch_value


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


class DeploymentParameters(BaseModel):
    database: dbstr
    database_user: dbstr
    database_password: dbstr
    database_size: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=GIB)]
    storage_size: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=GIB)]
    vcpu: Annotated[int, Field(gt=0, le=2**31 - 1)]
    memory: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=GIB)]
    iops: Annotated[int, Field(ge=100, le=2**31 - 1)]
    database_image_tag: Literal["15.1.0.147"]


class DeploymentStatus(BaseModel):
    status: StatusType


async def create_vela_config(branch_id: Identifier, parameters: DeploymentParameters, branch: Slug):
    namespace = deployment_namespace(branch_id)
    logging.info(
        "Creating Vela configuration for namespace: %s (database %s, user %s, branch %s, branch_id=%s)",
        namespace,
        parameters.database,
        parameters.database_user,
        branch,
        branch_id,
    )

    chart = resources.files(__package__) / "charts" / "supabase"
    compose_file = Path(__file__).with_name("compose.yml")
    if not compose_file.exists():
        raise FileNotFoundError(f"docker-compose manifest not found at {compose_file}")
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
    db_spec.setdefault("storagePersistence", {})["size"] = f"{bytes_to_gib(parameters.storage_size)}Gi"
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag
    storage_spec = values_content.setdefault("storage", {})
    storage_spec["enabled"] = True
    storage_persistence = storage_spec.setdefault("persistence", {})
    storage_persistence["enabled"] = True
    storage_persistence["size"] = f"{bytes_to_gib(parameters.storage_size)}Gi"
    namespace = deployment_namespace(branch_id)

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
                    "--set-file",
                    f"composeYaml={compose_file}",
                    "-f",
                    temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create deployment: {e.stderr}")
            release_name = _release_name(namespace)
            await check_output(
                ["helm", "uninstall", release_name, "-n", namespace],
                stderr=subprocess.PIPE,
                text=True,
            )
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
    release = _release_name(namespace)
    await asyncio.to_thread(
        subprocess.check_call,
        ["helm", "uninstall", release, "-n", namespace, "--wait"],
    )

    await kube_service.delete_namespace(namespace)


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


class ResizeParameters(BaseModel):
    database_size: Annotated[int, Field(gt=0, multiple_of=GIB)] | None
    storage_size: Annotated[int, Field(gt=0, multiple_of=GIB)] | None


def resize_deployment(branch_id: Identifier, parameters: ResizeParameters):
    """Perform an in-place Helm upgrade to disk. Only parameters provided will be updated.
    others are preserved using --reuse-values.
    """
    chart = resources.files(__package__) / "charts" / "supabase"
    # Minimal values file with only overrides
    values_content: dict = {}
    db_spec = values_content.setdefault("db", {})
    if parameters.database_size is not None:
        db_spec.setdefault("persistence", {})["size"] = f"{bytes_to_gib(parameters.database_size)}Gi"
    if parameters.storage_size is not None:
        storage_size_gi = f"{bytes_to_gib(parameters.storage_size)}Gi"
        db_spec.setdefault("storagePersistence", {})["size"] = storage_size_gi
        storage_spec = values_content.setdefault("storage", {})
        storage_spec["enabled"] = True
        storage_persistence = storage_spec.setdefault("persistence", {})
        storage_persistence["enabled"] = True
        storage_persistence["size"] = storage_size_gi

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
    branch_id: Identifier
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
                "konghq.com/plugins": "realtime-cors",
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

    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name="supabase-supabase-rest",
            service_port=3000,
            path_prefix="/rest",
            route_suffix="postgrest-route",
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
            path_prefix="/meta",
            route_suffix="pgmeta-route",
        ),
    ]


async def _apply_http_routes(namespace: str, routes: list[dict[str, Any]]) -> None:
    """Apply HTTPRoute manifests without blocking the event loop."""
    try:
        await kube_service.apply_http_routes(namespace, routes)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise BranchEndpointError(f"Failed to apply HTTPRoute: {exc}") from exc


def _build_kong_plugin(namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "configuration.konghq.com/v1",
        "kind": "KongPlugin",
        "metadata": {
            "name": "realtime-cors",
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


async def _apply_kong_plugin(namespace: str, plugin: dict[str, Any]) -> None:
    """Apply KongPlugin manifest without blocking the event loop."""
    try:
        await kube_service.apply_kong_plugin(namespace, plugin)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise BranchEndpointError(f"Failed to apply KongPlugin: {exc}") from exc


async def provision_branch_endpoints(
    spec: BranchEndpointProvisionSpec,
    *,
    ref: str,
) -> BranchEndpointResult:
    """Provision DNS + HTTPRoute resources (PostgREST + Storage + Realtime + PGMeta) for a branch."""

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

    # Apply the KongPlugin for CORS
    kong_plugin = _build_kong_plugin(gateway_cfg.namespace)
    await _apply_kong_plugin(gateway_cfg.namespace, kong_plugin)

    route_specs = (
        _postgrest_route_specs(ref, domain, gateway_cfg.namespace)
        + _storage_route_specs(ref, domain, gateway_cfg.namespace)
        + _realtime_route_specs(ref, domain, gateway_cfg.namespace)
        + _pgmeta_route_specs(ref, domain, gateway_cfg.namespace)
    )
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
    await create_vela_config(branch_id, parameters, branch_slug)

    # Provision DNS + HTTPRoute resources
    ref = branch_dns_label(branch_id)
    await provision_branch_endpoints(
        spec=BranchEndpointProvisionSpec(
            project_id=project_id,
            branch_id=branch_id,
            branch_slug=branch_slug,
        ),
        ref=ref,
    )
