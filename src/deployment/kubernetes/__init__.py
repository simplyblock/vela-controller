import logging
import math
from copy import deepcopy
from typing import Any

from aiohttp import ClientError
from kubernetes_asyncio import client

from ...exceptions import VelaKubernetesError
from ._util import core_v1_client, custom_api_client, storage_v1_client
from .neonvm import NeonVM, get_neon_vm

logger = logging.getLogger(__name__)


class KubernetesService:
    async def delete_namespace(self, namespace: str) -> None:
        async with core_v1_client() as core_v1:
            await core_v1.delete_namespace(name=namespace)

    async def ensure_namespace(self, namespace: str) -> None:
        async with core_v1_client() as core_v1:
            body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
            try:
                await core_v1.create_namespace(body=body)
            except client.exceptions.ApiException as exc:
                if exc.status != 409:
                    raise

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

    async def apply_kong_consumer(self, namespace: str, consumer: dict[str, Any]) -> None:
        group, version = consumer["apiVersion"].split("/")
        plural = "kongconsumers"

        async with custom_api_client() as custom:
            try:
                await custom.create_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    body=consumer,
                )
                logger.info("Created KongConsumer %s in %s", consumer["metadata"]["name"], namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 409:
                    logger.info(
                        "KongConsumer %s already exists in %s; replacing",
                        consumer["metadata"]["name"],
                        namespace,
                    )
                    await custom.replace_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        name=consumer["metadata"]["name"],
                        body=consumer,
                    )
                else:
                    raise

    async def apply_secret(self, namespace: str, secret: dict[str, Any]) -> None:
        name = secret["metadata"]["name"]
        async with core_v1_client() as core_v1:
            try:
                await core_v1.create_namespaced_secret(namespace=namespace, body=secret)
                logger.info("Created Secret %s in %s", name, namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 409:
                    logger.info("Secret %s already exists in %s; replacing", name, namespace)
                    await core_v1.replace_namespaced_secret(
                        name=name,
                        namespace=namespace,
                        body=secret,
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

    async def delete_storage_class(self, name: str) -> None:
        async with storage_v1_client() as storage_v1:
            try:
                await storage_v1.delete_storage_class(name)
                logger.info("Deleted StorageClass %s", name)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    logger.info("StorageClass %s not found; skipping delete", name)
                    return
                raise

    async def get_service_node_port(
        self,
        namespace: str,
        name: str,
    ) -> int:
        """
        Return the NodePort for the specified service port (or first port when unspecified).
        """
        service = await self.get_service(namespace, name)
        return service.spec.ports[0].node_port

    async def get_service(self, namespace: str, name: str) -> Any:
        async with core_v1_client() as core_v1:
            try:
                return await core_v1.read_namespaced_service(name=name, namespace=namespace)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"Service {namespace!r}/{name!r} not found") from exc
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

    async def resize_pvc_storage(self, namespace: str, name: str, storage: str) -> None:
        async with core_v1_client() as core_v1:
            try:
                await core_v1.patch_namespaced_persistent_volume_claim(
                    name=name,
                    namespace=namespace,
                    body={"spec": {"resources": {"requests": {"storage": storage}}}},
                )
            except client.exceptions.ApiException as exc:
                detail = exc.body or exc.reason or str(exc)
                raise VelaKubernetesError(
                    f"Failed to resize PVC {namespace!r}/{name!r} to {storage}: {detail}"
                ) from exc

        logger.info("Resized PVC %s/%s to %s", namespace, name, storage)

    async def get_persistent_volume(self, name: str) -> Any:
        async with core_v1_client() as core_v1:
            try:
                return await core_v1.read_persistent_volume(name=name)
            except client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise VelaKubernetesError(f"PersistentVolume {name!r} not found") from exc
                raise

    async def resize_autoscaler_vm(
        self,
        namespace: str,
        name: str,
        *,
        cpu_milli: int | None,
        memory_bytes: int | None,
    ) -> NeonVM:
        """
        Patch the Neon autoscaler VM guest resources.
        """
        vm = await get_neon_vm(namespace, name)
        guest = vm.guest

        vm_manifest = _build_autoscaler_vm_manifest(vm.model_dump(by_alias=True), namespace, name)
        guest_spec = vm_manifest.setdefault("spec", {}).setdefault("guest", {})
        cpu_block = guest_spec.setdefault("cpus", {})
        min_milli = guest.cpus.min_milli
        max_milli = guest.cpus.max_milli
        use_milli = cpu_milli if cpu_milli is not None else guest.cpus.use_milli
        cpu_block["min"] = _milli_to_cores(min_milli)
        cpu_block["max"] = _milli_to_cores(max_milli)
        cpu_block["use"] = _milli_to_cores(use_milli)

        if memory_bytes is not None:
            slot_size_bytes = guest.slot_size_bytes
            if slot_size_bytes <= 0:
                raise VelaKubernetesError("Autoscaler VM memory slot size is invalid")

            min_slots = guest.memory_slots.min_int
            max_slots = guest.memory_slots.max_int
            target_slots = math.ceil(memory_bytes / slot_size_bytes)
            desired_slots = max(min_slots, target_slots)

            if desired_slots > max_slots:
                raise VelaKubernetesError(f"Requested autoscaler memory exceeds configured maximum slots ({max_slots})")

            current_usage = guest.memory_slots.use_int * slot_size_bytes
            if current_usage is not None and memory_bytes < current_usage:
                raise VelaKubernetesError(
                    "Requested autoscaler memory is lower than current utilization; downsizing is not permitted"
                )

            guest_spec.setdefault("memorySlots", {})["use"] = desired_slots
            guest_spec.setdefault("memorySlots", {})["limit"] = desired_slots

        return await self.apply_autoscaler_vm(namespace, name, vm_manifest)

    async def apply_autoscaler_vm(self, namespace: str, name: str, vm_manifest: dict[str, Any]) -> NeonVM:
        """
        Apply (create or patch) the Neon autoscaler VM using a merge patch.
        """
        manifest = deepcopy(vm_manifest)

        try:
            async with custom_api_client() as custom_client:
                applied = await custom_client.patch_namespaced_custom_object(
                    group="vm.neon.tech",
                    version="v1",
                    namespace=namespace,
                    plural="virtualmachines",
                    name=name,
                    body=manifest,
                    _content_type="application/merge-patch+json",
                )
        except client.exceptions.ApiException as exc:
            raise VelaKubernetesError(f"Failed to apply autoscaler VM {namespace!r}/{name!r}: {exc.reason}") from exc
        except ClientError as exc:
            raise VelaKubernetesError(f"Autoscaler VM apply for {namespace!r}/{name!r} failed") from exc

        return NeonVM.model_validate(applied)


def _milli_to_cores(value: int) -> int | float:
    cores = value / 1000
    return int(cores) if cores.is_integer() else cores


def _build_autoscaler_vm_manifest(vm_obj: dict[str, Any], namespace: str, name: str) -> dict[str, Any]:
    """
    Prepare a clean autoscaler VM manifest for patching.
    """
    return {
        "apiVersion": vm_obj.get("apiVersion", "vm.neon.tech/v1"),
        "kind": vm_obj.get("kind", "VirtualMachine"),
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": deepcopy(vm_obj.get("spec") or {}),
    }
