from typing import Any

from kubernetes_asyncio import client as kubernetes_client
from kubernetes_asyncio.client.exceptions import ApiException

from ..._util import Identifier
from ...exceptions import VelaKubernetesError
from .. import branch_storage_class_name, kube_service
from ._util import core_v1_client
from ._wait import wait_for_condition


async def delete_pvc(namespace: str, name: str) -> None:
    """Delete a PVC if it exists, ignoring not-found responses."""
    async with core_v1_client() as api:
        try:
            await api.delete_namespaced_persistent_volume_claim(
                namespace=namespace,
                name=name,
                body=kubernetes_client.V1DeleteOptions(),
            )
        except ApiException as exc:
            if exc.status != 404:
                raise VelaKubernetesError(f"Failed to delete PVC {namespace}/{name}: {exc.body or exc}") from exc


async def create_pvc(namespace: str, pvc: kubernetes_client.V1PersistentVolumeClaim) -> None:
    """Create a PVC from the supplied manifest."""
    async with core_v1_client() as api:
        try:
            await api.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
        except ApiException as exc:
            raise VelaKubernetesError(
                f"Failed to create PVC {namespace}/{pvc.metadata.name}: {exc.body or exc}"
            ) from exc


async def wait_for_pvc_absent(
    namespace: str,
    name: str,
    *,
    timeout: float,
    poll_interval: float,
) -> None:
    """Block until the PVC has been deleted."""
    await wait_for_condition(
        fetch=lambda: _read_pvc(namespace, name),
        is_ready=lambda result: result is None,
        timeout=timeout,
        poll_interval=poll_interval,
        not_found_message=None,
        timeout_message=f"Timed out waiting for PVC {namespace}/{name} deletion",
    )


async def wait_for_pvc_bound(
    namespace: str,
    name: str,
    *,
    timeout: float,
    poll_interval: float,
) -> None:
    """Block until the PVC reaches phase=Bound."""
    await wait_for_condition(
        fetch=lambda: _read_pvc(namespace, name),
        is_ready=lambda pvc: bool(
            (phase := getattr(getattr(pvc, "status", None), "phase", None)) and str(phase).upper() == "BOUND"
        ),
        timeout=timeout,
        poll_interval=poll_interval,
        not_found_message=f"PVC {namespace}/{name} not found while waiting for binding",
        timeout_message=f"Timed out waiting for PVC {namespace}/{name} to bind",
    )


def build_pvc_manifest_from_existing(
    pvc: Any,
    *,
    branch_id: Identifier,
    volume_snapshot_name: str,
) -> kubernetes_client.V1PersistentVolumeClaim:
    """Build a PVC manifest equivalent to the input but sourced from the given snapshot."""
    metadata = getattr(pvc, "metadata", None)
    spec = getattr(pvc, "spec", None)
    if metadata is None or spec is None:
        raise VelaKubernetesError("PersistentVolumeClaim missing metadata/spec")

    labels = dict(getattr(metadata, "labels", None) or {})
    annotations = dict(getattr(metadata, "annotations", None) or {})
    for noisy in (
        "pv.kubernetes.io/bind-completed",
        "pv.kubernetes.io/bound-by-controller",
        "volume.beta.kubernetes.io/storage-provisioner",
        "kubectl.kubernetes.io/last-applied-configuration",
    ):
        annotations.pop(noisy, None)

    access_modes = list(getattr(spec, "access_modes", None) or [])
    storage_class_name = getattr(spec, "storage_class_name", None) or getattr(spec, "storageClassName", None)
    if not storage_class_name:
        storage_class_name = branch_storage_class_name(branch_id)

    resources = getattr(spec, "resources", None)
    requests = getattr(resources, "requests", {}) if resources else {}
    storage_request = requests.get("storage")
    if storage_request is None:
        raise VelaKubernetesError("PersistentVolumeClaim missing storage request")

    data_source = kubernetes_client.V1TypedLocalObjectReference(
        api_group="snapshot.storage.k8s.io",
        kind="VolumeSnapshot",
        name=volume_snapshot_name,
    )
    data_source_ref = kubernetes_client.V1TypedObjectReference(
        api_group="snapshot.storage.k8s.io",
        kind="VolumeSnapshot",
        name=volume_snapshot_name,
    )
    resource_requirements = kubernetes_client.V1ResourceRequirements(requests={"storage": str(storage_request)})

    pvc_spec = kubernetes_client.V1PersistentVolumeClaimSpec(
        access_modes=access_modes or None,
        data_source=data_source,
        resources=resource_requirements,
        storage_class_name=storage_class_name,
        volume_mode=getattr(spec, "volume_mode", None) or getattr(spec, "volumeMode", None),
    )
    if hasattr(pvc_spec, "data_source_ref"):
        pvc_spec.data_source_ref = data_source_ref

    pvc_name = getattr(metadata, "name", None)
    if not pvc_name:
        raise VelaKubernetesError("PersistentVolumeClaim missing metadata.name")

    metadata_obj = kubernetes_client.V1ObjectMeta(
        name=pvc_name,
        labels=labels or None,
        annotations=annotations or None,
    )

    return kubernetes_client.V1PersistentVolumeClaim(
        metadata=metadata_obj,
        spec=pvc_spec,
    )


async def _read_pvc(namespace: str, name: str):
    """Return the PVC object or None when it does not exist."""
    try:
        return await kube_service.get_persistent_volume_claim(namespace, name)
    except VelaKubernetesError as exc:
        if "not found" in str(exc).lower():
            return None
        raise
