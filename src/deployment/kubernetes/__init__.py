import logging
from typing import Any

from kubernetes_asyncio import client

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

    async def get_vm_pod_name(self, namespace: str, vm_name: str) -> str:
        """
        Resolve the virt-launcher pod name backing the supplied KubeVirt VirtualMachine.

        The lookup relies on the `vm.kubevirt.io/name` label set by KubeVirt on the
        launcher pod. Returns the first pod that is not terminating and still has a
        valid metadata.name.
        """
        label_selector = f"vm.kubevirt.io/name={vm_name}"
        async with core_v1_client() as core_v1:
            try:
                pods = await core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
            except client.exceptions.ApiException as exc:
                raise VelaKubernetesError(
                    f"Failed to list pods backing VM {vm_name!r} in namespace {namespace!r}"
                ) from exc

        if not pods.items:
            raise VelaKubernetesError(f"No pods found for VM {vm_name!r} in namespace {namespace!r}")

        for pod in pods.items:
            metadata = pod.metadata
            if metadata and metadata.name and not metadata.deletion_timestamp:
                logger.debug("Resolved VM %s pod to %s", vm_name, metadata.name)
                return metadata.name

        first_pod = pods.items[0].metadata.name if pods.items[0].metadata else None
        if first_pod:
            logger.debug("Using first available pod %s for VM %s despite termination status", first_pod, vm_name)
            return first_pod

        raise VelaKubernetesError(f"Unable to resolve a pod name for VM {vm_name!r} in namespace {namespace!r}")

    async def resize_vm_compute_cpu(self, namespace: str, vm_name: str, *, cpu_request: str, cpu_limit: str) -> None:
        """
        Patch the virt-launcher pod backing a VirtualMachine using the resize subresource
        so that the `compute` container reflects the requested CPU request/limit values.
        """
        pod_name = await self.get_vm_pod_name(namespace, vm_name)
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
