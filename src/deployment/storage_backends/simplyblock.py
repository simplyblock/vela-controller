from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.util import await_only
from ulid import ULID

from .base import (
    Identifier,
    Snapshot,
    SnapshotDetails,
    SnapshotRef,
    StorageBackend,
    StorageCapabilitiesPublic,
    Volume,
    VolumeCapabilities,
    VolumeGroup,
    VolumeQosProfile,
    VolumeSpec,
    VolumeUsage,
)
from ..._util import IOPS_MIN, quantity_to_bytes
from ...exceptions import VelaDeploymentError, VelaKubernetesError, VelaSimplyblockAPIError
from .. import (
    AUTOSCALER_PVC_SUFFIX,
    AUTOSCALER_WAL_PVC_SUFFIX,
    STORAGE_PVC_SUFFIX,
    ensure_branch_storage_class,
    get_autoscaler_vm_identity,
    kube_service,
    update_branch_volume_iops,
)
from ..kubernetes.pvc import delete_pvc, wait_for_pvc_absent
from ..kubernetes.snapshot import create_snapshot_from_pvc, delete_snapshot, read_snapshot, wait_snapshot_ready
from ..kubernetes.volume_clone import clone_branch_database_volume, restore_branch_database_volume_from_snapshot
from ..settings import Settings, get_settings
from ..simplyblock_api import create_simplyblock_api

_SNAPSHOT_TIMEOUT_SECONDS = float(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
_SNAPSHOT_POLL_INTERVAL_SECONDS = float(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))
_PVC_TIMEOUT_SECONDS = float(600)
_PVC_POLL_INTERVAL_SECONDS = float(2)
_K8S_NAME_MAX_LENGTH = 63

_CAPABILITIES = VolumeCapabilities(
    supports_dynamic_provisioning=True,
    supports_storage_class_per_branch=True,
    supports_storage_class_shared=True,
    supports_topology_awareness=False, # possible but not yet implemented
    supports_encrypted_volumes=False, # possible but not yet implemented
    supports_vm_live_migration=True,
    supports_usage_qos_metrics=True,
    supports_qos_read_write_split=False, # possible but not yet implemented
    supports_usage_storage_metrics=True,
    supports_file_storage_volume=True,
    supports_pitr_wal_volume=True,
    supports_snapshot_content_rebind=True,
    supports_clone_without_snapshot=True,
    supports_fast_clone=True,
    supports_backup_snapshot_labels=True,
    supports_restore_size_discovery=True,
    supports_volume_groups=True,
    supports_consistency_group_snapshots=False, # possible but not yet implemented
    supports_volume_group_iops=False, # possible but not yet implemented
    supports_volume_group_iops_update=False, # possible but not yet implemented
    supports_volume_group_throughput=False, # possible but not yet implemented
    supports_volume_group_throughput_update=False, # possible but not yet implemented
    supports_volume_group_usage_qos_metrics=False, # possible but not yet implemented
    supports_volume_group_usage_storage_metrics=False, # possible but not yet implemented
    supports_volume_iops=True,
    supports_volume_iops_update=True,
    supports_volume_throughput=False, # possible but not yet implemented
    supports_volume_throughput_update=False, # possible but not yet implemented
    supports_volume_usage_qos_metrics=True,
    supports_volume_usage_storage_metrics=True,
    supports_volume_clone_cross_namespace=True,
    supports_volume_expansion=True,
    supports_volume_expansion_online=True,
    supports_volume_relocation=False, # not required
)


def _build_storage_class_manifest(*, storage_class_name: str, iops: int, base_storage_class: Any) -> dict[str, Any]:
    provisioner = getattr(base_storage_class, "provisioner", None)
    if not provisioner:
        raise VelaKubernetesError("Base storage class missing provisioner")

    base_parameters = dict(getattr(base_storage_class, "parameters", {}) or {})
    cluster_id = base_parameters.get("cluster_id")
    if not cluster_id:
        raise VelaKubernetesError("Base storage class missing required parameter 'cluster_id'")

    parameters = {key: str(value) for key, value in base_parameters.items()}
    parameters.update(
        {
            "qos_rw_iops": str(iops),
            "qos_rw_mbytes": "0",
            "qos_r_mbytes": "0",
            "qos_w_mbytes": "0",
        }
    )

    allow_volume_expansion = getattr(base_storage_class, "allow_volume_expansion", None)
    volume_binding_mode = getattr(base_storage_class, "volume_binding_mode", None)
    reclaim_policy = getattr(base_storage_class, "reclaim_policy", None)
    mount_options = getattr(base_storage_class, "mount_options", None)

    manifest: dict[str, Any] = {
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {
            "name": storage_class_name,
        },
        "provisioner": provisioner,
        "parameters": parameters,
    }
    if reclaim_policy is not None:
        manifest["reclaimPolicy"] = reclaim_policy
    if volume_binding_mode is not None:
        manifest["volumeBindingMode"] = volume_binding_mode
    if allow_volume_expansion is not None:
        manifest["allowVolumeExpansion"] = bool(allow_volume_expansion)
    if mount_options:
        manifest["mountOptions"] = list(mount_options)

    return manifest


def _release_name() -> str:
    return get_settings().deployment_release_name


def _release_fullname() -> str:
    release = _release_name()
    return release if "vela" in release else f"{release}-vela"


def _autoscaler_vm_name() -> str:
    name = f"{_release_fullname()}-autoscaler-vm"
    return name[:63].rstrip("-")


async def _resolve_volume_identifiers(namespace: str, pvc_name: str) -> tuple[UUID, UUID | None]:
    pvc = await kube_service.get_persistent_volume_claim(namespace, pvc_name)
    pvc_spec = getattr(pvc, "spec", None)
    volume_name = getattr(pvc_spec, "volume_name", None) if pvc_spec else None
    if not volume_name:
        raise VelaDeploymentError(f"PersistentVolumeClaim {namespace}/{pvc_name} is not bound to a PersistentVolume")

    pv = await kube_service.get_persistent_volume(volume_name)
    pv_spec = getattr(pv, "spec", None)
    csi_spec = getattr(pv_spec, "csi", None) if pv_spec else None
    volume_attributes = getattr(csi_spec, "volume_attributes", None) if csi_spec else None
    if not isinstance(volume_attributes, dict):
        raise VelaDeploymentError(
            f"PersistentVolume {volume_name} missing CSI volume attributes; cannot resolve Simplyblock volume UUID"
        )
    volume_uuid = volume_attributes.get("uuid")
    volume_cluster_id = volume_attributes.get("cluster_id")
    if not volume_uuid:
        raise VelaDeploymentError(f"PersistentVolume {volume_name} missing 'uuid' attribute in CSI volume attributes")
    return UUID(volume_uuid), UUID(volume_cluster_id) if volume_cluster_id is not None else None


async def resolve_storage_volume_identifiers(namespace: str) -> tuple[UUID, UUID | None]:
    pvc_name = f"{_autoscaler_vm_name()}{STORAGE_PVC_SUFFIX}"
    return await _resolve_volume_identifiers(namespace, pvc_name)


async def resolve_autoscaler_volume_identifiers(namespace: str) -> tuple[UUID, UUID | None]:
    pvc_name = f"{_autoscaler_vm_name()}{AUTOSCALER_PVC_SUFFIX}"
    return await _resolve_volume_identifiers(namespace, pvc_name)


async def resolve_autoscaler_wal_volume_identifiers(namespace: str) -> tuple[UUID, UUID | None]:
    pvc_name = f"{_autoscaler_vm_name()}{AUTOSCALER_WAL_PVC_SUFFIX}"
    return await _resolve_volume_identifiers(namespace, pvc_name)


@dataclass
class SimplyblockVolume(Volume):
    identifier: Identifier
    _backend: "SimplyblockBackend"

    async def resize(self, new_size_bytes: int) -> None:
        await self._backend._resize_volume(self.identifier, new_size_bytes)
        self.size_bytes = new_size_bytes

    async def delete(self) -> None:
        await self._backend._delete_volume(self.identifier)

    async def snapshot(self, label: str, backup_id: Identifier) -> "SimplyblockSnapshot":
        details = await self._backend._snapshot_volume(
            namespace=self.namespace,
            pvc_name=self.pvc_name,
            label=label,
            backup_id=backup_id,
        )
        snapshot_ref = SnapshotRef(name=details.name, namespace=details.namespace, content_name=details.content_name)
        return SimplyblockSnapshot(details=details, snapshot_ref=snapshot_ref, source_identifier=self.identifier, _backend=self._backend)

    async def update_performance(self, qos: VolumeQosProfile) -> None:
        await self._backend._update_volume_performance(self.identifier, qos)

    async def usage(self) -> VolumeUsage | None:
        return await self._backend._get_volume_usage(self.identifier)

    async def get_usage(self) -> VolumeUsage | None:
        return await self.usage()

    async def relocate(self, target_node: str | None = None) -> None:
        raise VelaDeploymentError("simplyblock backend does not require volume relocation")


@dataclass
class SimplyblockStoragePoolVolumeGroup(VolumeGroup):
    _backend: "SimplyblockBackend"

    async def delete(self) -> None:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def update_performance(self, qos: VolumeQosProfile) -> None:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def volumes(self) -> list[Volume]:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def snapshot(self, label: str, backup_id: Identifier) -> Snapshot:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def get_usage(self) -> VolumeUsage | None:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")


@dataclass
class SimplyblockSimpleVolumeGroup(VolumeGroup):
    _backend: "SimplyblockBackend"
    identifier: Identifier

    async def delete(self) -> None:
        volumes = await self._backend._list_volume_group_volumes(self.identifier)
        for volume in volumes:
            await volume.delete()

    async def update_performance(self, qos: VolumeQosProfile) -> None:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def volumes(self) -> list[Volume]:
        return self._backend._list_volume_group_volumes(self.identifier)

    async def snapshot(self, label: str, backup_id: Identifier) -> Snapshot:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def get_usage(self) -> VolumeUsage | None:
        raise VelaDeploymentError("simplyblock backend does not expose volume groups")

    async def provision_volume(
            self,
            name: str,
            size_bytes: int,
            qos: VolumeQosProfile | None = None,
    ) -> Volume:
        return await self._backend._provision_volume(self.identifier, name, size_bytes, qos)


@dataclass
class SimplyblockSnapshot(Snapshot):
    source_identifier: Identifier | None
    _backend: "SimplyblockBackend"

    async def delete(self) -> None:
        await self._backend._delete_snapshot(self.details)

    async def restore_to(self, target_volume: Volume, database_size: int) -> None:
        target_identifier = self._backend._volume_identifier(target_volume)
        if self.source_identifier is None:
            raise VelaDeploymentError("Cannot restore snapshot without source identifier context")
        await self._backend._restore_volume_from_snapshot(
            source_identifier=self.source_identifier,
            target_identifier=target_identifier,
            snapshot_ref=self.snapshot_ref,
            database_size=database_size,
        )

    async def clone_to(self, target_volume: Volume, new_size: int) -> None:
        target_identifier = self._backend._volume_identifier(target_volume)
        if self.source_identifier is None:
            raise VelaDeploymentError("Cannot clone snapshot without source identifier context")
        await self._backend._clone_volume_from_snapshot(
            source_identifier=self.source_identifier,
            target_identifier=target_identifier,
            new_size=new_size,
        )


class SimplyblockBackend(StorageBackend):
    name = "simplyblock"

    def __init__(self, settings: Settings):
        self.settings = settings

    def get_capabilities(self) -> StorageCapabilitiesPublic:
        return StorageCapabilitiesPublic(
            backend="simplyblock",
            capabilities=_CAPABILITIES,
            storage_class=self.settings.storage_default_class,
            snapshot_class=self.settings.storage_snapshot_class,
            qos_policy=self.settings.storage_qos_policy,
        )

    def resolve_storage_class(self) -> str:
        return self.settings.storage_default_class

    def resolve_snapshot_class(self) -> str:
        return self.settings.storage_snapshot_class

    async def provision_volume(
        self,
        name: str,
        size_bytes: int,
        qos: VolumeQosProfile | None = None,
    ) -> Volume:
        return await self._provision_volume(ULID(), name, size_bytes, qos)

    async def provision_volume_group(
        self,
        group_id: Identifier,
        group_name: str,
        qos: VolumeQosProfile | None = None,
    ) -> VolumeGroup:
        return SimplyblockSimpleVolumeGroup(identifier=group_id, _backend=self, name=group_name)

    async def lookup_volume(self, volume_id: Identifier) -> Volume | None:
        namespace, autoscaler_vm_name = get_autoscaler_vm_identity(volume_id)
        pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        try:
            pvc = await kube_service.get_persistent_volume_claim(namespace, pvc_name)
        except VelaKubernetesError as exc:
            if self._is_not_found_error(exc):
                return None
            raise

        requests = getattr(getattr(getattr(pvc, "spec", None), "resources", None), "requests", None) or {}
        size_bytes = quantity_to_bytes(requests.get("storage")) or 0
        return SimplyblockVolume(
            namespace=namespace,
            pvc_name=pvc_name,
            storage_class=self._branch_storage_class_name(volume_id),
            size_bytes=size_bytes,
            identifier=volume_id,
            _backend=self,
        )

    async def lookup_volume_group(self, group_id: Identifier) -> VolumeGroup | None:
        return SimplyblockSimpleVolumeGroup(identifier=group_id, _backend=self, name=str(group_id))

    async def lookup_snapshot(self, snapshot_ref: SnapshotRef) -> Snapshot | None:
        snapshot = await read_snapshot(snapshot_ref.namespace, snapshot_ref.name)
        if snapshot is None:
            return None
        status = snapshot.get("status") or {}
        content_name_payload = status.get("boundVolumeSnapshotContentName")
        content_name = content_name_payload if isinstance(content_name_payload, str) else snapshot_ref.content_name
        details = SnapshotDetails(
            name=snapshot_ref.name,
            namespace=snapshot_ref.namespace,
            content_name=content_name,
            size_bytes=quantity_to_bytes(status.get("restoreSize")),
        )
        resolved_ref = SnapshotRef(name=details.name, namespace=details.namespace, content_name=details.content_name)
        return SimplyblockSnapshot(details=details, snapshot_ref=resolved_ref, source_identifier=None, _backend=self)

    def validate_qos_profile(self, qos: VolumeQosProfile) -> None:
        unsupported_fields = [
            field_name
            for field_name in (
                "max_read_iops",
                "max_write_iops",
                "max_read_mibps",
                "max_write_mibps",
                "max_read_write_mibps",
            )
            if getattr(qos, field_name) is not None
        ]
        if unsupported_fields and self.settings.storage_qos_policy == "strict":
            raise VelaDeploymentError(
                f"simplyblock backend supports only max_read_write_iops; unsupported fields: {', '.join(unsupported_fields)}"
            )

    def validate_capabilities_for_operation(self, operation: str, params: dict[str, Any] | None = None) -> None:
        capabilities = self.get_capabilities().capabilities
        checks = {
            "snapshots": capabilities.supports_snapshots,
            "snapshot_restore": capabilities.supports_snapshot_restore,
            "volume_expansion": capabilities.supports_volume_expansion,
            "runtime_iops_update": capabilities.supports_volume_iops_update,
            "volume_group_provision": capabilities.supports_volume_groups,
            "volume_relocation": capabilities.supports_volume_relocation,
        }
        supported = checks.get(operation)
        if supported is False:
            raise VelaDeploymentError(f"Operation {operation!r} is not supported by backend {self.name!r}")

    async def _provision_volume(
        self,
        identifier: Identifier,
        name: str,
        size_bytes: int,
        qos: VolumeQosProfile | None = None,
    ) -> Volume:
        self.validate_qos_profile(qos or VolumeQosProfile())
        iops = self._effective_iops(qos)
        storage_class = await ensure_branch_storage_class(identifier, iops=iops)
        namespace, autoscaler_vm_name = get_autoscaler_vm_identity(identifier)
        pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        return SimplyblockVolume(
            namespace=namespace,
            pvc_name=pvc_name,
            storage_class=storage_class,
            size_bytes=size_bytes,
            identifier=identifier,
            _backend=self,
        )

    async def _resize_volume(self, identifier: Identifier, new_size_bytes: int) -> None:
        # Resize is currently handled by existing PVC workflows outside this adapter.
        # TODO: Move update of the PVC here
        _ = (identifier, new_size_bytes)
        return None

    async def _delete_volume(self, identifier: Identifier) -> None:
        namespace, autoscaler_vm_name = get_autoscaler_vm_identity(identifier)
        pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"

        pv_name: str | None
        try:
            pvc = await kube_service.get_persistent_volume_claim(namespace, pvc_name)
            pvc_spec = getattr(pvc, "spec", None)
            pv_name = getattr(pvc_spec, "volume_name", None) if pvc_spec else None
        except VelaKubernetesError as exc:
            if self._is_not_found_error(exc):
                return
            raise

        await delete_pvc(namespace, pvc_name)
        await wait_for_pvc_absent(
            namespace,
            pvc_name,
            timeout=_PVC_TIMEOUT_SECONDS,
            poll_interval=_PVC_POLL_INTERVAL_SECONDS,
        )
        if pv_name:
            await kube_service.delete_persistent_volume(pv_name)

    async def _snapshot_volume(
        self,
        namespace: str,
        pvc_name: str,
        label: str | None = None,
        backup_id: Identifier | None = None,
    ) -> SnapshotDetails:
        snapshot_name = self._build_snapshot_name(
            label=label or "backup",
            backup_id=backup_id or str(ULID()),
        )
        await create_snapshot_from_pvc(
            namespace=namespace,
            name=snapshot_name,
            snapshot_class=self.resolve_snapshot_class(),
            pvc_name=pvc_name,
        )
        snapshot = await wait_snapshot_ready(
            namespace,
            snapshot_name,
            timeout=_SNAPSHOT_TIMEOUT_SECONDS,
            poll_interval=_SNAPSHOT_POLL_INTERVAL_SECONDS,
        )
        status = snapshot.get("status") or {}
        content_name_payload = status.get("boundVolumeSnapshotContentName")
        content_name = content_name_payload if isinstance(content_name_payload, str) else None
        return SnapshotDetails(
            name=snapshot_name,
            namespace=namespace,
            content_name=content_name,
            size_bytes=quantity_to_bytes(status.get("restoreSize")),
        )

    async def _clone_volume_from_snapshot(
        self,
        source_identifier: Identifier,
        target_identifier: Identifier,
        new_size: int,
    ) -> None:
        storage_class = await ensure_branch_storage_class(target_identifier, iops=IOPS_MIN)
        await clone_branch_database_volume(
            source_branch_id=source_identifier,
            target_branch_id=target_identifier,
            snapshot_class=self.resolve_snapshot_class(),
            storage_class_name=storage_class,
            snapshot_timeout_seconds=_SNAPSHOT_TIMEOUT_SECONDS,
            snapshot_poll_interval_seconds=_SNAPSHOT_POLL_INTERVAL_SECONDS,
            pvc_timeout_seconds=_PVC_TIMEOUT_SECONDS,
            pvc_poll_interval_seconds=_PVC_POLL_INTERVAL_SECONDS,
            database_size=new_size,
            pitr_enabled=False,
        )

    async def _restore_volume_from_snapshot(
        self,
        source_identifier: Identifier,
        target_identifier: Identifier,
        snapshot_ref: SnapshotRef,
        database_size: int,
    ) -> None:
        storage_class = await ensure_branch_storage_class(target_identifier, iops=IOPS_MIN)
        await restore_branch_database_volume_from_snapshot(
            source_branch_id=source_identifier,
            target_branch_id=target_identifier,
            snapshot_namespace=snapshot_ref.namespace,
            snapshot_name=snapshot_ref.name,
            snapshot_content_name=snapshot_ref.content_name,
            snapshot_class=self.resolve_snapshot_class(),
            storage_class_name=storage_class,
            database_size=database_size,
            snapshot_timeout_seconds=_SNAPSHOT_TIMEOUT_SECONDS,
            snapshot_poll_interval_seconds=_SNAPSHOT_POLL_INTERVAL_SECONDS,
            pvc_timeout_seconds=_PVC_TIMEOUT_SECONDS,
            pvc_poll_interval_seconds=_PVC_POLL_INTERVAL_SECONDS,
        )

    async def _update_volume_performance(self, identifier: Identifier, qos: VolumeQosProfile) -> None:
        self.validate_qos_profile(qos)
        await update_branch_volume_iops(identifier, self._effective_iops(qos))

    async def _get_volume_usage(self, identifier: Identifier) -> VolumeUsage | None:
        namespace = f"{self.settings.deployment_namespace_prefix}-{str(identifier).lower()}"
        try:
            volume, _ = await resolve_autoscaler_volume_identifiers(namespace)
            async with create_simplyblock_api() as sb_api:
                stats = await sb_api.volume_iostats(volume=volume)
        except (VelaDeploymentError, VelaSimplyblockAPIError, VelaKubernetesError):
            return None

        return VolumeUsage(
            used_bytes=int(stats.get("size_used") or 0),
            read_iops=int(stats.get("read_io_ps") or 0),
            write_iops=int(stats.get("write_io_ps") or 0),
        )

    async def _delete_snapshot(self, details: SnapshotDetails) -> None:
        await delete_snapshot(details.namespace, details.name)

    def _volume_identifier(self, volume: Volume) -> Identifier:
        if isinstance(volume, SimplyblockVolume):
            return volume.identifier
        raise VelaDeploymentError("Snapshot operation requires a SimplyblockVolume target")

    def _effective_iops(self, qos: VolumeQosProfile | None) -> int:
        iops = (qos.max_read_write_iops if qos is not None else None) or IOPS_MIN
        return max(iops, IOPS_MIN)

    def _build_snapshot_name(self, *, label: str, backup_id: str) -> str:
        clean_label = re.sub(r"[^a-z0-9-]", "-", label.lower())
        clean_label = re.sub(r"-+", "-", clean_label).strip("-") or "backup"
        clean_backup = re.sub(r"[^a-z0-9-]", "-", backup_id.lower())
        clean_backup = re.sub(r"-+", "-", clean_backup).strip("-") or datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        return f"{clean_label}-{clean_backup}"[:_K8S_NAME_MAX_LENGTH].strip("-")

    def _branch_storage_class_name(self, identifier: Identifier) -> str:
        return f"sc-{str(identifier).lower()}"

    def _is_not_found_error(self, exc: VelaKubernetesError) -> bool:
        return "not found" in str(exc).lower()
