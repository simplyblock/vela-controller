import logging
import math
import os
import subprocess
import tempfile
import textwrap
from importlib import resources
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from cloudflare import AsyncCloudflare, CloudflareError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import BaseModel, Field

from .._util import (
    CPU_CONSTRAINTS,
    DATABASE_SIZE_CONSTRAINTS,
    IOPS_CONSTRAINTS,
    MEMORY_CONSTRAINTS,
    STORAGE_SIZE_CONSTRAINTS,
    Identifier,
    Slug,
    StatusType,
    bytes_to_gib,
    bytes_to_mib,
    check_output,
    dbstr,
)
from ..exceptions import VelaCloudflareError, VelaKubernetesError
from .kubernetes import KubernetesService
from .kubevirt import get_virtualmachine_status
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()

DEFAULT_DATABASE_VM_NAME = "supabase-supabase-db"
CHECK_ENCRYPTED_HEADER_PLUGIN_NAME = "check-x-connection-encrypted"


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


def inject_branch_env(compose: dict[str, Any], branch_id: Identifier) -> dict[str, Any]:
    try:
        vector_service = compose["services"]["vector"]
    except KeyError as e:
        raise RuntimeError("Failed to inject branch env into compose file: missing services.vector") from e

    vector_env = vector_service.setdefault("environment", {})
    vector_env["LOGFLARE_PUBLIC_ACCESS_TOKEN"] = os.environ.get("LOGFLARE_PUBLIC_ACCESS_TOKEN", "")
    vector_env["NAMESPACE"] = os.environ.get("VELA_DEPLOYMENT_NAMESPACE_PREFIX", "")
    vector_env["VELA_BRANCH"] = str(branch_id).lower()

    return compose


class DeploymentParameters(BaseModel):
    database: dbstr
    database_user: dbstr
    database_password: dbstr
    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS)]
    storage_size: Annotated[int, Field(**STORAGE_SIZE_CONSTRAINTS)]
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS)]  # units of milli vCPU
    memory_bytes: Annotated[int, Field(**MEMORY_CONSTRAINTS)]
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS)]
    database_image_tag: Literal["15.1.0.147"]


class DeploymentStatus(BaseModel):
    status: StatusType


async def create_vela_config(
    branch_id: Identifier,
    parameters: DeploymentParameters,
    branch: Slug,
    jwt_secret: str,
    anon_key: str,
    service_key: str,
):
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
    compose_file_path = Path(__file__).with_name("compose.yml")
    if not compose_file_path.exists():
        raise FileNotFoundError(f"docker-compose manifest not found at {compose_file_path}")

    compose_file = yaml.safe_load(compose_file_path.read_text())
    compose_file = inject_branch_env(compose_file, branch_id)

    vector_file = Path(__file__).with_name("vector.yml")
    if not vector_file.exists():
        raise FileNotFoundError(f"vector config file not found at {vector_file}")
    values_content = yaml.safe_load((chart / "values.yaml").read_text())

    # Override defaults
    values_content.setdefault("secret", {}).setdefault("jwt", {}).update(
        secret=jwt_secret,
        anonKey=anon_key,
        serviceKey=service_key,
    )

    db_secrets = values_content.setdefault("db", {}).setdefault("credentials", {})
    db_secrets["adminusername"] = parameters.database_user
    db_secrets["adminpassword"] = parameters.database_password
    db_secrets["admindb"] = parameters.database
    db_secrets["pgmeta_crypto_key"] = settings.pgmeta_crypto_key

    db_spec = values_content.setdefault("db", {})
    resource_cfg = db_spec.setdefault("resources", {})

    cpu_provisioning_factor = 0.25  # request = 25% of limit
    memory_request_fraction = 0.90  # request = 90% of limit

    resource_cfg["limits"] = {
        "cpu": f"{parameters.milli_vcpu}m",
        "memory": f"{bytes_to_mib(parameters.memory_bytes)}Mi",
    }
    resource_cfg["requests"] = {
        "cpu": f"{int(parameters.milli_vcpu * cpu_provisioning_factor)}m",
        "memory": f"{bytes_to_mib(math.floor(parameters.memory_bytes * memory_request_fraction))}Mi",
    }

    db_spec.setdefault("persistence", {})["size"] = f"{bytes_to_gib(parameters.database_size)}Gi"
    db_spec.setdefault("storagePersistence", {})["size"] = f"{bytes_to_gib(parameters.storage_size)}Gi"
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag
    storage_spec = values_content.setdefault("storage", {})
    storage_spec["enabled"] = True
    storage_persistence = storage_spec.setdefault("persistence", {})
    storage_persistence["enabled"] = True
    storage_persistence["size"] = f"{bytes_to_gib(parameters.storage_size)}Gi"

    # todo: create an storage class with the given IOPS
    values_content["provisioning"] = {"iops": parameters.iops}
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
    try:
        await kube_service.delete_namespace(namespace)
    except ApiException as exc:
        if exc.status == 404:
            logger.info("Namespace %s not found", namespace)
            return
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


class ResizeParameters(BaseModel):
    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS)] | None
    storage_size: Annotated[int, Field(**STORAGE_SIZE_CONSTRAINTS)] | None


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
    plugins: list[str] = Field(default_factory=lambda: ["realtime-cors"])


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

    path = f"/platform/pgmeta/{ref}"
    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,  # TODO: change domain to api.
            namespace=namespace,
            service_name="supabase-supabase-meta",
            service_port=8080,
            path_prefix=path,
            route_suffix="pgmeta-route",
            plugins=["realtime-cors", CHECK_ENCRYPTED_HEADER_PLUGIN_NAME],
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
        _build_realtime_cors_plugin(namespace),
        _build_check_encrypted_header_plugin(namespace),
    ]


def _build_realtime_cors_plugin(namespace: str) -> dict[str, Any]:
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

    # Apply the KongPlugins required for the branch routes
    kong_plugins = _build_kong_plugins(gateway_cfg.namespace)
    for plugin in kong_plugins:
        await _apply_kong_plugin(gateway_cfg.namespace, plugin)

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
    jwt_secret: str,
    anon_key: str,
    service_key: str,
) -> None:
    """Background task: provision infra for a branch and persist the resulting endpoint."""

    # Create the main deployment (database etc)
    await create_vela_config(
        project_id, parameters, branch_slug, jwt_secret=jwt_secret, anon_key=anon_key, service_key=service_key
    )

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
