from typing import Any

from kubernetes_asyncio import client as kubernetes_client
from kubernetes_asyncio.client.exceptions import ApiException

from ...exceptions import VelaKubernetesError
from ._util import custom_api_client
from ._wait import wait_for_condition


async def create_snapshot_from_pvc(
    *,
    namespace: str,
    name: str,
    snapshot_class: str,
    pvc_name: str,
) -> None:
    """Create a VolumeSnapshot that references an existing PVC."""
    manifest = _snapshot_manifest(
        name=name,
        snapshot_class=snapshot_class,
        source={"persistentVolumeClaimName": pvc_name},
    )
    await _create_snapshot(namespace, manifest)


async def create_snapshot_from_content(
    *,
    namespace: str,
    name: str,
    snapshot_class: str,
    content_name: str,
) -> None:
    """Create a VolumeSnapshot binding to a pre-existing VolumeSnapshotContent."""
    manifest = _snapshot_manifest(
        name=name,
        snapshot_class=snapshot_class,
        source={"volumeSnapshotContentName": content_name},
    )
    await _create_snapshot(namespace, manifest)


async def create_snapshot_content_from_handle(
    *,
    name: str,
    driver: str,
    snapshot_handle: str,
    snapshot_class: str,
    snapshot_namespace: str,
    snapshot_name: str,
) -> None:
    """Materialize a VolumeSnapshotContent that references a CSI snapshot handle."""
    manifest = {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshotContent",
        "metadata": {"name": name},
        "spec": {
            "driver": driver,
            "deletionPolicy": "Delete",
            "source": {"snapshotHandle": snapshot_handle},
            "volumeSnapshotClassName": snapshot_class,
            "volumeSnapshotRef": {
                "name": snapshot_name,
                "namespace": snapshot_namespace,
            },
        },
    }
    async with custom_api_client() as api:
        try:
            await api.create_cluster_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                plural="volumesnapshotcontents",
                body=manifest,
            )
        except ApiException as exc:
            raise VelaKubernetesError(f"Failed to create VolumeSnapshotContent {name}: {exc.body or exc}") from exc


async def read_snapshot(namespace: str, name: str) -> dict[str, Any] | None:
    """Fetch a VolumeSnapshot object or return None if it does not exist."""
    async with custom_api_client() as api:
        try:
            return await api.get_namespaced_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                namespace=namespace,
                plural="volumesnapshots",
                name=name,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise VelaKubernetesError(f"Failed to read VolumeSnapshot {namespace}/{name}: {exc.body or exc}") from exc


async def read_snapshot_content(name: str) -> dict[str, Any] | None:
    """Fetch a VolumeSnapshotContent object or return None if it does not exist."""
    async with custom_api_client() as api:
        try:
            return await api.get_cluster_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                plural="volumesnapshotcontents",
                name=name,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise VelaKubernetesError(f"Failed to read VolumeSnapshotContent {name}: {exc.body or exc}") from exc


async def delete_snapshot(namespace: str, name: str) -> None:
    """Delete the provided VolumeSnapshot if present."""
    async with custom_api_client() as api:
        try:
            await api.delete_namespaced_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                namespace=namespace,
                plural="volumesnapshots",
                name=name,
                body=kubernetes_client.V1DeleteOptions(),
            )
        except ApiException as exc:
            if exc.status != 404:
                raise VelaKubernetesError(
                    f"Failed to delete VolumeSnapshot {namespace}/{name}: {exc.body or exc}"
                ) from exc


async def delete_snapshot_content(name: str) -> None:
    """Delete the provided VolumeSnapshotContent if present."""
    async with custom_api_client() as api:
        try:
            await api.delete_cluster_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                plural="volumesnapshotcontents",
                name=name,
                body=kubernetes_client.V1DeleteOptions(),
            )
        except ApiException as exc:
            if exc.status != 404:
                raise VelaKubernetesError(f"Failed to delete VolumeSnapshotContent {name}: {exc.body or exc}") from exc


async def ensure_snapshot_absent(
    namespace: str,
    name: str,
    *,
    timeout: float,
    poll_interval: float,
) -> None:
    """Ensure the named VolumeSnapshot no longer exists, waiting for deletion if required."""
    snapshot = await read_snapshot(namespace, name)
    if snapshot is None:
        return
    await delete_snapshot(namespace, name)
    await wait_for_condition(
        fetch=lambda: read_snapshot(namespace, name),
        is_ready=lambda result: result is None,
        timeout=timeout,
        poll_interval=poll_interval,
        not_found_message=None,
        timeout_message=f"Timed out waiting for VolumeSnapshot {namespace}/{name} deletion",
    )


async def ensure_snapshot_content_absent(
    name: str,
    *,
    timeout: float,
    poll_interval: float,
) -> None:
    """Ensure the named VolumeSnapshotContent no longer exists, waiting for deletion if required."""
    content = await read_snapshot_content(name)
    if content is None:
        return
    await delete_snapshot_content(name)
    await wait_for_condition(
        fetch=lambda: read_snapshot_content(name),
        is_ready=lambda result: result is None,
        timeout=timeout,
        poll_interval=poll_interval,
        not_found_message=None,
        timeout_message=f"Timed out waiting for VolumeSnapshotContent {name} deletion",
    )


async def wait_snapshot_ready(
    namespace: str,
    name: str,
    *,
    timeout: float,
    poll_interval: float,
) -> dict[str, Any]:
    """Wait for the specified VolumeSnapshot to report readyToUse=True."""
    return await wait_for_condition(
        fetch=lambda: read_snapshot(namespace, name),
        is_ready=lambda snapshot: bool((snapshot or {}).get("status", {}).get("readyToUse")),
        timeout=timeout,
        poll_interval=poll_interval,
        not_found_message=f"VolumeSnapshot {namespace}/{name} not found while waiting for readiness",
        timeout_message=f"Timed out waiting for VolumeSnapshot {namespace}/{name} to become ready",
    )


def _snapshot_manifest(*, name: str, snapshot_class: str, source: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {"name": name},
        "spec": {"volumeSnapshotClassName": snapshot_class, "source": source},
    }


async def _create_snapshot(namespace: str, manifest: dict[str, Any]) -> None:
    async with custom_api_client() as api:
        try:
            await api.create_namespaced_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                namespace=namespace,
                plural="volumesnapshots",
                body=manifest,
            )
        except ApiException as exc:
            raise VelaKubernetesError(
                f"Failed to create VolumeSnapshot {namespace}/{manifest['metadata']['name']}: {exc.body or exc}"
            ) from exc
