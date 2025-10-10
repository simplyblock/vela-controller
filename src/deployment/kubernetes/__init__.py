import logging
from typing import Any

from kubernetes_asyncio import client

from ._util import core_v1_client, custom_api_client

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
