import logging
from typing import Any

from kubernetes import client, config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KubernetesService:
    def __init__(self):
        try:
            # Try to load in-cluster config first (for running in a pod)
            config.load_incluster_config()
        except config.config_exception.ConfigException:
            try:
                # Fall back to kubeconfig file (for local development)
                config.load_kube_config()
            except config.config_exception.ConfigException:
                logger.error("Could not configure kubernetes python client")
                raise

        self.core_v1 = client.CoreV1Api()
        self.custom = client.CustomObjectsApi()

    def delete_namespace(self, namespace: str):
        self.core_v1.delete_namespace(namespace)

    def check_namespace_status(self, namespace) -> dict[str, str]:
        """
        Check if all pods in the namespace are running

        Raises
        - KeyError if namespace is missing
        - urllib3.exceptions.HTTPError on failed access to the kubernetes API
        - kubernetes.client.rest.ApiException on API failure
        """
        if namespace not in {namespace.metadata.name for namespace in self.core_v1.list_namespace().items}:
            raise KeyError(f"Namespace {namespace} not found")
        return {pod.metadata.name: pod.status.phase for pod in self.core_v1.list_namespaced_pod(namespace).items}

    def apply_http_routes(self, namespace: str, routes: list[dict[str, Any]]):
        for route in routes:
            group, version = route["apiVersion"].split("/")
            plural = "httproutes"

            try:
                self.custom.create_namespaced_custom_object(
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
                    self.custom.replace_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        name=route["metadata"]["name"],
                        body=route,
                    )
                else:
                    raise
