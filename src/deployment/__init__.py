import asyncio
import logging
import subprocess
import tempfile
from importlib import resources
from typing import Annotated, Any, Literal

import httpx
import ulid
import yaml
from kubernetes.client.rest import ApiException
from pydantic import BaseModel, Field
from urllib3.exceptions import HTTPError

from .._util import Slug, check_output, dbstr
from .kubernetes import KubernetesService
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()


def _default_branch_slug() -> Slug:
    # Local import to avoid import cycle with models.project -> deployment
    from ..api.models.branch import Branch

    return Branch.DEFAULT_SLUG


def _deployment_namespace(id_: int, branch: Slug) -> str:
    branch = branch or _default_branch_slug()
    return f"{settings.deployment_namespace_prefix}-deployment-{id_}-{branch}"


def get_deployment_namespace(id_: int, branch: Slug) -> str:
    """Public helper to compute the kubernetes namespace for a project branch."""

    return _deployment_namespace(id_, branch)


def _release_name(namespace: str) -> str:
    return f"supabase-{namespace}"


class DeploymentParameters(BaseModel):
    database: dbstr
    database_user: dbstr
    database_password: dbstr
    database_size: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=2**30)]
    vcpu: Annotated[int, Field(gt=0, le=2**31 - 1)]
    memory: Annotated[int, Field(gt=0, le=2**63 - 1, multiple_of=2**30)]
    iops: Annotated[int, Field(gt=0, le=2**31 - 1)]
    database_image_tag: Literal["15.1.0.147"]


StatusType = Literal["ACTIVE_HEALTHY", "ACTIVE_UNHEALTHY", "COMING_UP", "INACTIVE", "UNKNOWN"]


class DeploymentStatus(BaseModel):
    status: StatusType
    pods: dict[str, str]
    message: str


async def create_vela_config(id_: int, parameters: DeploymentParameters, branch: Slug):
    logging.info(
        f"Creating Vela configuration for namespace: {_deployment_namespace(id_, branch)}"
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
    db_spec["ram"] = parameters.memory // (2**30)
    db_spec.setdefault("persistence", {})["size"] = f"{parameters.database_size // (2**30)}Gi"
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag
    namespace = _deployment_namespace(id_, branch)

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


def get_deployment_status(id_: int, branch: Slug) -> DeploymentStatus:
    status: StatusType

    try:
        k8s_statuses = kube_service.check_namespace_status(_deployment_namespace(id_, branch))

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


def delete_deployment(id_: int, branch: Slug):
    namespace = _deployment_namespace(id_, branch)
    subprocess.check_call(["helm", "uninstall", _release_name(namespace), "-n", namespace, "--wait"])
    kube_service.delete_namespace(namespace)


def get_db_vmi_identity(id_: int, branch: Slug) -> tuple[str, str]:
    """
    Return the (namespace, vmi_name) for the project's database VirtualMachineInstance.

    The Helm chart defines the DB VM fullname as "{Release.Name}-{ChartName}-db" when no overrides
    are provided. Our release name is "supabase-{namespace}" and chart name is "supabase".
    Hence the VMI name resolves to: f"{_release_name(namespace)}-supabase-db".
    """
    namespace = _deployment_namespace(id_, branch)
    vmi_name = f"{_release_name(namespace)}-supabase-db"
    return namespace, vmi_name


class BranchEndpointError(RuntimeError):
    """Raised when provisioning branch endpoints fails."""


class CloudflareConfig(BaseModel):
    api_token: str
    zone_id: str
    dns_target: str
    domain_suffix: str

    @classmethod
    def from_env(cls) -> "CloudflareConfig":
        token = settings.cloudflare_api_token
        zone_id = settings.cloudflare_zone_id
        dns_target = settings.cloudflare_dns_target
        domain_suffix = settings.cloudflare_domain_suffix

        if not token or not zone_id or not dns_target or not domain_suffix:
            raise BranchEndpointError("Cloudflare credentials are not configured")

        return cls(api_token=token, zone_id=zone_id, dns_target=dns_target, domain_suffix=domain_suffix)


class KubeGatewayConfig(BaseModel):
    namespace: str = Field(default="vela-deployment-1-main")
    gateway_name: str = Field(default="public-gateway")
    gateway_namespace: str = Field(default="kong-system")

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
    project_id: int
    branch_slug: str


class BranchEndpointResult(BaseModel):
    ref: str
    domain: str
    namespace: str


async def _create_dns_record(cf: CloudflareConfig, domain: str) -> None:
    headers = {
        "Authorization": f"Bearer {cf.api_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "type": "A",
        "name": domain,
        "content": cf.dns_target,
        "ttl": 120,
        "proxied": False,
    }

    url = f"https://api.cloudflare.com/client/v4/zones/{cf.zone_id}/dns_records"

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            resp = await client.post(url, headers=headers, json=payload)
    except httpx.HTTPError as exc:
        raise BranchEndpointError(f"Cloudflare request failed: {exc}") from exc

    try:
        body = resp.json()
    except ValueError as exc:
        raise BranchEndpointError(f"Invalid Cloudflare response: {resp.text}") from exc

    if resp.status_code != 200 or not body.get("success"):
        raise BranchEndpointError(f"Cloudflare API error: {resp.text}")
    logger.info("Created DNS A record %s -> %s", domain, cf.dns_target)


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


def _branch_route_specs(ref: str, domain: str, namespace: str) -> list[HTTPRouteSpec]:
    base_service = f"supabase-{namespace}-supabase"
    return [
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name=f"{base_service}-meta",
            service_port=8080,
            path_prefix="/meta",
            route_suffix="meta-route",
        ),
        HTTPRouteSpec(
            ref=ref,
            domain=domain,
            namespace=namespace,
            service_name=f"{base_service}-rest",
            service_port=3000,
            path_prefix="/rest",
            route_suffix="rest-route",
        ),
    ]


async def provision_branch_endpoints(
    spec: BranchEndpointProvisionSpec,
    *,
    ref: str,
) -> BranchEndpointResult:
    cf_cfg = CloudflareConfig.from_env()
    gateway_cfg = KubeGatewayConfig().for_namespace(get_deployment_namespace(spec.project_id, spec.branch_slug))

    domain = f"{ref}.{cf_cfg.domain_suffix}".lower()
    logger.info(
        "Provisioning endpoints for project_id=%s branch=%s domain=%s",
        spec.project_id,
        spec.branch_slug,
        domain,
    )

    await _create_dns_record(cf_cfg, domain)

    route_specs = _branch_route_specs(ref, domain, gateway_cfg.namespace)
    routes = [_build_http_route(gateway_cfg, route_spec) for route_spec in route_specs]
    try:
        await asyncio.to_thread(kube_service.apply_http_routes, gateway_cfg.namespace, routes)
    except Exception as exc:  # pragma: no cover - surfaced to caller
        raise BranchEndpointError(f"Failed to apply HTTPRoute: {exc}") from exc

    return BranchEndpointResult(ref=ref, domain=domain, namespace=gateway_cfg.namespace)


async def deploy_branch_environment(
    id_: int,
    parameters: DeploymentParameters,
    branch: Slug,
) -> BranchEndpointResult:
    """Create the Helm deployment for a branch, then provision its external endpoints."""

    ref = str(ulid.new()).lower()
    await create_vela_config(id_, parameters, branch)
    return await provision_branch_endpoints(BranchEndpointProvisionSpec(project_id=id_, branch_slug=branch), ref=ref)


async def deploy_branch_environment_background(
    *,
    project_id: int,
    branch_id: int,
    branch_slug: Slug,
    parameters: DeploymentParameters,
) -> None:
    try:
        result = await deploy_branch_environment(project_id, parameters, branch_slug)
    except BranchEndpointError:
        logger.exception(
            "Failed provisioning endpoints for project %s branch %s",
            project_id,
            branch_slug,
        )
        return
    except Exception:
        logger.exception(
            "Failed deploying branch infrastructure for project %s branch %s",
            project_id,
            branch_slug,
        )
        return

    from sqlalchemy.ext.asyncio import AsyncSession

    from ..api.db import engine
    from ..api.models.branch import Branch

    async with AsyncSession(engine) as background_session:
        branch_obj = await background_session.get(Branch, branch_id)
        if branch_obj is None:
            logger.warning(
                "Provisioned branch infrastructure for missing branch id=%s project=%s",
                branch_id,
                project_id,
            )
            return

        branch_obj.endpoint_domain = result.domain
        branch_obj.endpoint_namespace = result.namespace
        branch_obj.external_id = result.ref
        await background_session.commit()
