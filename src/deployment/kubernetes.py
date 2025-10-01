import logging
import time
from collections.abc import Mapping
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

    def get_kubevirt_config(self, namespace: str = "kubevirt", name: str = "kubevirt") -> dict[str, Any]:
        return self.custom.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="kubevirts",
            name=name,
        )

    def get_virtual_machine(self, namespace: str, name: str) -> dict[str, Any]:
        return self.custom.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachines",
            name=name,
        )

    def get_virtual_machine_instance(self, namespace: str, name: str) -> dict[str, Any]:
        return self.custom.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachineinstances",
            name=name,
        )

    def get_vmi_memory_status(self, namespace: str, name: str) -> dict[str, str] | None:
        vmi = self.get_virtual_machine_instance(namespace, name)
        status = vmi.get("status", {}) if isinstance(vmi, Mapping) else {}
        memory = status.get("memory") if isinstance(status, Mapping) else None
        if isinstance(memory, Mapping):
            # Shallow copy so callers can mutate without affecting cache
            return {str(k): str(v) for k, v in memory.items()}
        return None

    def wait_for_vmi_guest_requested(
        self,
        namespace: str,
        name: str,
        expected_quantity: str,
        *,
        timeout_seconds: int = 30,
        interval_seconds: int = 2,
    ) -> dict[str, str]:
        deadline = time.time() + timeout_seconds
        memory_status: dict[str, str] | None = None
        while time.time() < deadline:
            memory_status = self.get_vmi_memory_status(namespace, name)
            if memory_status and memory_status.get("guestRequested") == expected_quantity:
                return memory_status
            time.sleep(interval_seconds)

        if not memory_status:
            memory_status = self.get_vmi_memory_status(namespace, name) or {}
        raise RuntimeError(
            f"Timed out waiting for VMI {name} in {namespace} to report guestRequested={expected_quantity}; "
            f"last observed status: {memory_status}"
        )
