import asyncio
import logging
from typing import Any

from kubernetes_asyncio import client, config
from kubernetes_asyncio.config import ConfigException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KubernetesService:
    def __init__(self):
        self._core_v1: client.CoreV1Api | None = None
        self._custom_api: client.CustomObjectsApi | None = None
        self._lock = asyncio.Lock()

    async def _ensure_clients(self) -> None:
        if self._core_v1 and self._custom_api:
            return

        async with self._lock:
            if self._core_v1 and self._custom_api:
                return

            try:
                # Try to load in-cluster config first (for running in a pod)
                config.load_incluster_config()
            except ConfigException:
                try:
                    # Fall back to kubeconfig file (for local development)
                    await config.load_kube_config()
                except ConfigException:
                    logger.error("Could not configure kubernetes python client")
                    raise

            self._core_v1 = client.CoreV1Api()
            self._custom_api = client.CustomObjectsApi()

    @property
    def core_v1(self) -> client.CoreV1Api:
        if self._core_v1 is None:
            raise RuntimeError("Kubernetes client not initialized; call an async method first")
        return self._core_v1

    @property
    def custom(self) -> client.CustomObjectsApi:
        if self._custom_api is None:
            raise RuntimeError("Kubernetes client not initialized; call an async method first")
        return self._custom_api

    async def delete_namespace(self, namespace: str) -> None:
        await self._ensure_clients()
        await self.core_v1.delete_namespace(name=namespace)

    async def check_namespace_status(self, namespace: str) -> dict[str, str]:
        """
        Check if all pods in the namespace are running.

        Raises
        - KeyError if namespace is missing
        - urllib3.exceptions.HTTPError on failed access to the kubernetes API
        - kubernetes_asyncio.client.rest.ApiException on API failure
        """

        await self._ensure_clients()
        namespaces = await self.core_v1.list_namespace()
        if namespace not in {ns.metadata.name for ns in namespaces.items}:
            raise KeyError(f"Namespace {namespace} not found")

        pods = await self.core_v1.list_namespaced_pod(namespace)
        return {pod.metadata.name: pod.status.phase for pod in pods.items}

    async def apply_http_routes(self, namespace: str, routes: list[dict[str, Any]]) -> None:
        await self._ensure_clients()
        for route in routes:
            group, version = route["apiVersion"].split("/")
            plural = "httproutes"

            try:
                await self.custom.create_namespaced_custom_object(
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
                    await self.custom.replace_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        name=route["metadata"]["name"],
                        body=route,
                    )
                else:
                    raise
