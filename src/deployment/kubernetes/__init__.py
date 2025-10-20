import logging
from datetime import datetime
from typing import Any

from aiohttp import ClientError
from kubernetes_asyncio import client

from ..._util import quantity_to_bytes
from ...exceptions import VelaKubernetesError
from ._util import core_v1_client, custom_api_client, storage_v1_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KubernetesService:
    async def delete_namespace(self, namespace: str) -> None:
        async with core_v1_client() as core_v1:
            await core_v1.delete_namespace(name=namespace)

    async def check_namespace_status(self, namespace: str) -> dict[str, str]:
        """
        Check if all pods in the namespace are running.

        Raises
        - KeyError if namespace is missing
        - urllib3.exceptions.HTTPError on failed access to the kubernetes API
        - kubernetes_asyncio.client.rest.ApiException on API failure
        """

        async with core_v1_client() as core_v1:
            namespaces = await core_v1.list_namespace()

            if namespace not in {ns.metadata.name for ns in namespaces.items}:
                raise KeyError(f"Namespace {namespace} not found")

            pods = await core_v1.list_namespaced_pod(namespace)
        return {pod.metadata.name: pod.status.phase for pod in pods.items}

    async def apply_http_routes(self, namespace: str, routes: list[dict[str, Any]]) -> None:
        async with custom_api_client() as custom:
            for route in routes:
                group, version = route["apiVersion"].split("/")
                plural = "httproutes"

                try:
                    await custom.create_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        body=route,
                    )
                    logger.info("Created HTTPRoute %s in %s", route["metadata"]["name"], namespace)
                except client.exceptions.ApiException as exc:
                    if exc.status == 409:
                        logger.info(
                            "HTTPRoute %s already exists in %s; replacing",
                            route["metadata"]["name"],
                            namespace,
                        )
                        await custom.replace_namespaced_custom_object(
                            group=group,
                            version=version,
                            namespace=namespace,
                            plural=plural,
                            name=route["metadata"]["name"],
                            body=route,
                        )
                    else:
                        raise

    async def apply_kong_plugin(self, namespace: str, plugin: dict[str, Any]) -> None:
        group, version = plugin["apiVersion"].split("/")
        plural = "kongplugins"

        async with custom_api_client() as custom:
            try:
                await custom.create_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    body=plugin,
                )
                logger.info("Created KongPlugin %s in %s", plugin["metadata"]["name"], namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 409:
                    logger.info(
                        "KongPlugin %s already exists in %s; replacing",
                        plugin["metadata"]["name"],
                        namespace,
                    )
                    await custom.replace_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        name=plugin["metadata"]["name"],
                        body=plugin,
                    )
                else:
                    raise

    async def apply_storage_class(self, manifest: dict[str, Any]) -> None:
        name = manifest["metadata"]["name"]
        async with storage_v1_client() as storage_v1:
            try:
                await storage_v1.create_storage_class(body=manifest)
                logger.info("Created StorageClass %s", name)
            except client.exceptions.ApiException as exc:
                if exc.status == 409:
                    logger.info("StorageClass %s already exists; replacing", name)
                    await storage_v1.replace_storage_class(name=name, body=manifest)
                else:
                    raise

    async def get_storage_class(self, name: str) -> Any:
        async with storage_v1_client() as storage_v1:
            try:
                return await storage_v1.read_storage_class(name)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"StorageClass {name!r} not found") from exc
                raise

    async def get_config_map(self, namespace: str, name: str) -> Any:
        async with core_v1_client() as core_v1:
            try:
                return await core_v1.read_namespaced_config_map(name=name, namespace=namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"ConfigMap {namespace!r}/{name!r} not found") from exc
                raise

    async def get_secret(self, namespace: str, name: str) -> Any:
        async with core_v1_client() as core_v1:
            try:
                return await core_v1.read_namespaced_secret(name=name, namespace=namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"Secret {namespace!r}/{name!r} not found") from exc
                raise

    async def get_persistent_volume_claim(self, namespace: str, name: str) -> Any:
        async with core_v1_client() as core_v1:
            try:
                return await core_v1.read_namespaced_persistent_volume_claim(name=name, namespace=namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"PersistentVolumeClaim {namespace!r}/{name!r} not found") from exc
                raise

    async def get_persistent_volume(self, name: str) -> Any:
        async with core_v1_client() as core_v1:
            try:
                return await core_v1.read_persistent_volume(name=name)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"PersistentVolume {name!r} not found") from exc
                raise

    async def get_vm_pod_name(self, namespace: str, vm_name: str) -> tuple[str, str | None]:
        """
        Resolve the virt-launcher pod name backing the supplied KubeVirt VirtualMachine.

        Prefers running pods whose compute container is already ready. Falls back to the most
        recently started pod when no healthy candidates exist.
        """
        pods = await self.list_pods(namespace, f"vm.kubevirt.io/name={vm_name}")
        candidates = _eligible_vm_pods(pods.items or [])
        if not candidates:
            raise VelaKubernetesError(f"No pods found for VM {vm_name!r} in namespace {namespace!r}")

        target_pod = _choose_vm_pod(candidates)
        requested_memory = _compute_container_memory(target_pod)
        return target_pod.metadata.name, requested_memory

    async def list_pods(self, namespace, label):
        async with core_v1_client() as core_v1:
            try:
                pods = await core_v1.list_namespaced_pod(namespace=namespace, label_selector=label)
            except client.exceptions.ApiException as exc:
                raise VelaKubernetesError(f"Failed to list pods backing {label!r} in namespace {namespace!r}") from exc

        if not pods.items:
            raise VelaKubernetesError(f"No pods found for {label!r} in namespace {namespace!r}")
        return pods

    async def get_vm_memory_bytes(self, namespace: str, vm_name: str) -> int | None:
        """
        Fetch the VirtualMachine spec and return the configured guest memory in bytes.

        Returns None when the VM cannot be retrieved or the memory settings are absent.
        """
        try:
            async with custom_api_client() as custom_client:
                vm_obj = await custom_client.get_namespaced_custom_object(
                    group="kubevirt.io",
                    version="v1",
                    namespace=namespace,
                    plural="virtualmachines",
                    name=vm_name,
                )
        except client.exceptions.ApiException as exc:
            if exc.status != 404:
                logger.debug(
                    "Failed to fetch VirtualMachine %s/%s while reading memory spec: %s",
                    namespace,
                    vm_name,
                    exc,
                )
            return None
        except ClientError as exc:
            logger.debug(
                "Failed to fetch VirtualMachine %s/%s while reading memory spec: %s",
                namespace,
                vm_name,
                exc,
            )
            return None

        return self._extract_vm_memory_bytes(vm_obj)

    @staticmethod
    def _extract_vm_memory_bytes(vm_obj: dict[str, Any]) -> int | None:
        spec = vm_obj.get("spec") or {}
        template = spec.get("template") or {}
        template_spec = template.get("spec") or {}
        domain = template_spec.get("domain") or {}

        memory_block = domain.get("memory") or {}
        guest = memory_block.get("guest")
        if guest:
            guest_bytes = quantity_to_bytes(guest)
            if guest_bytes is not None:
                return guest_bytes

        resources = domain.get("resources") or {}
        for source in (resources.get("limits") or {}, resources.get("requests") or {}):
            memory_value = source.get("memory")
            if memory_value:
                memory_bytes = quantity_to_bytes(memory_value)
                if memory_bytes is not None:
                    return memory_bytes

        return None

    async def resize_vm_compute_cpu(self, namespace: str, vm_name: str, *, cpu_request: str, cpu_limit: str) -> None:
        """
        Patch the virt-launcher pod backing a VirtualMachine using the resize subresource
        so that the `compute` container reflects the requested CPU request/limit values.
        """
        pod_ref = await self.get_vm_pod_name(namespace, vm_name)
        pod_name = pod_ref[0] if isinstance(pod_ref, tuple) else pod_ref
        logger.info(
            "Preparing CPU resize for VM %s in %s; target pod=%s request=%s limit=%s",
            vm_name,
            namespace,
            pod_name,
            cpu_request,
            cpu_limit,
        )
        body = {
            "spec": {
                "containers": [
                    {
                        "name": "compute",
                        "resources": {
                            "requests": {"cpu": cpu_request},
                            "limits": {"cpu": cpu_limit},
                        },
                    }
                ]
            }
        }

        async with core_v1_client() as core_v1:
            try:
                await core_v1.api_client.call_api(
                    "/api/v1/namespaces/{namespace}/pods/{name}/resize",
                    "PATCH",
                    path_params={"namespace": namespace, "name": pod_name},
                    header_params={"Content-Type": "application/strategic-merge-patch+json"},
                    body=body,
                    auth_settings=["BearerToken"],
                )
                logger.info(
                    "Patched compute container CPU for pod %s (VM %s) in %s: request=%s limit=%s",
                    pod_name,
                    vm_name,
                    namespace,
                    cpu_request,
                    cpu_limit,
                )
            except client.exceptions.ApiException as exc:
                raise VelaKubernetesError(
                    f"Failed to resize pod {pod_name!r} backing VM {vm_name!r} in namespace {namespace!r}"
                ) from exc


def _eligible_vm_pods(pods: list[Any]) -> list[Any]:
    eligible: list[Any] = []
    for pod in pods:
        metadata = getattr(pod, "metadata", None)
        if metadata and not getattr(metadata, "deletion_timestamp", None):
            eligible.append(pod)
    return eligible


def _choose_vm_pod(candidates: list[Any]) -> Any:
    ordered_buckets = [
        [pod for pod in candidates if _pod_phase(pod) == "RUNNING" and _compute_container_ready(pod)],
        [pod for pod in candidates if _pod_phase(pod) == "RUNNING"],
        [pod for pod in candidates if _pod_phase(pod) == "PENDING"],
        candidates,
    ]
    for bucket in ordered_buckets:
        if bucket:
            return max(bucket, key=_pod_start_time)
    raise VelaKubernetesError("No eligible pods available")


def _pod_phase(pod: Any) -> str:
    status = getattr(pod, "status", None)
    return (getattr(status, "phase", None) or "").upper()


def _compute_container_ready(pod: Any) -> bool:
    statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
    compute_statuses = [cs for cs in statuses if getattr(cs, "name", None) == "compute"]
    target_statuses = compute_statuses or statuses
    return all(bool(getattr(cs, "ready", False)) for cs in target_statuses) if target_statuses else False


def _pod_start_time(pod: Any) -> datetime:
    status = getattr(pod, "status", None)
    metadata = getattr(pod, "metadata", None)
    for value in (
        getattr(status, "start_time", None),
        getattr(status, "startTime", None),
        getattr(metadata, "creation_timestamp", None),
        getattr(metadata, "creationTimestamp", None),
    ):
        if value is not None:
            return value
    return datetime.min


def _compute_container_memory(pod: Any) -> str | None:
    spec = getattr(pod, "spec", None)
    containers = getattr(spec, "containers", None) or []
    fallback: str | None = None
    for container in containers:
        resources = getattr(container, "resources", None)
        if isinstance(resources, dict):
            requests = resources.get("requests") or {}
            limits = resources.get("limits") or {}
        else:
            requests = getattr(resources, "requests", None) or {}
            limits = getattr(resources, "limits", None) or {}
        memory_value = requests.get("memory") or limits.get("memory")
        if not memory_value:
            continue
        if getattr(container, "name", None) == "compute":
            return memory_value
        if fallback is None:
            fallback = memory_value
    return fallback
