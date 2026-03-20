from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

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
    VolumeUsage,
)
from ..._util import quantity_to_bytes
from ...exceptions import VelaDeploymentError, VelaKubernetesError
from .. import AUTOSCALER_PVC_SUFFIX, get_autoscaler_vm_identity, kube_service
from ..kubernetes.pvc import delete_pvc, wait_for_pvc_absent
from ..kubernetes.snapshot import create_snapshot_from_pvc, delete_snapshot, read_snapshot, wait_snapshot_ready
from ..kubernetes.volume_clone import clone_branch_database_volume, restore_branch_database_volume_from_snapshot
from ..settings import Settings

_SNAPSHOT_TIMEOUT_SECONDS = float(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
_SNAPSHOT_POLL_INTERVAL_SECONDS = float(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))
_PVC_TIMEOUT_SECONDS = float(600)
_PVC_POLL_INTERVAL_SECONDS = float(2)
_K8S_NAME_MAX_LENGTH = 63
_DEFAULT_TOPO_LVM_STORAGE_CLASS = "topolvm-provisioner"
_DEFAULT_TOPO_LVM_SNAPSHOT_CLASS = "topolvm-snapshotclass"
_TOPO_LVM_PROVISIONER_MARKERS = ("topolvm",)

_CAPABILITIES = VolumeCapabilities(
    supports_dynamic_provisioning=True,
    supports_storage_class_per_branch=False,
    supports_storage_class_shared=True,
    supports_topology_awareness=True,
    supports_encrypted_volumes=False,
    supports_vm_live_migration=False,
    supports_usage_qos_metrics=False,
    supports_qos_read_write_split=False,
    supports_usage_storage_metrics=False,
    supports_file_storage_volume=True,
    supports_pitr_wal_volume=True,
    supports_snapshot_content_rebind=True,
    supports_clone_without_snapshot=False,
    supports_fast_clone=False,
    supports_backup_snapshot_labels=True,
    supports_restore_size_discovery=True,
    supports_volume_groups=True,
    supports_consistency_group_snapshots=False,
    supports_volume_group_iops=False,
    supports_volume_group_iops_update=False,
    supports_volume_group_throughput=False,
    supports_volume_group_throughput_update=False,
    supports_volume_group_usage_qos_metrics=False,
    supports_volume_group_usage_storage_metrics=False,
    supports_volume_iops=False,
    supports_volume_iops_update=False,
    supports_volume_throughput=False,
    supports_volume_throughput_update=False,
    supports_volume_usage_qos_metrics=False,
    supports_volume_usage_storage_metrics=False,
    supports_volume_clone_cross_namespace=True,
    supports_volume_expansion=True,
    supports_volume_expansion_online=True,
    supports_volume_relocation=False,
)


@dataclass
class LvmVolume(Volume):
    identifier: Identifier
    _backend: "LvmBackend"

    async def resize(self, new_size_bytes: int) -> None:
        # Resize execution is handled by PVC workflows outside this backend currently.
        self.size_bytes = new_size_bytes

    async def delete(self) -> None:
        await self._backend._delete_volume(self.identifier)

    async def snapshot(self, label: str, backup_id: Identifier) -> "LvmSnapshot":
        details = await self._backend._snapshot_volume(self.namespace, self.pvc_name, label=label, backup_id=backup_id)
        snapshot_ref = SnapshotRef(name=details.name, namespace=details.namespace, content_name=details.content_name)
        return LvmSnapshot(details=details, snapshot_ref=snapshot_ref, source_identifier=self.identifier, _backend=self._backend)

    async def update_performance(self, qos: VolumeQosProfile) -> None:
        self._backend.validate_qos_profile(qos)

    async def usage(self) -> VolumeUsage | None:
        return None

    async def get_usage(self) -> VolumeUsage | None:
        return await self.usage()

    async def relocate(self, target_node: str | None = None) -> None:
        raise VelaDeploymentError("lvm backend does not support volume relocation")


@dataclass
class LvmVolumeGroup(VolumeGroup):
    identifier: Identifier
    _backend: "LvmBackend"

    async def delete(self) -> None:
        volumes = await self.volumes()
        for volume in volumes:
            await volume.delete()

    async def update_performance(self, qos: VolumeQosProfile) -> None:
        self._backend.validate_qos_profile(qos)

    async def volumes(self) -> list[Volume]:
        volume = await self._backend.lookup_volume(self.identifier)
        return [] if volume is None else [volume]

    async def snapshot(self, label: str, backup_id: Identifier) -> Snapshot:
        volume = await self._backend.lookup_volume(self.identifier)
        if volume is None:
            raise VelaDeploymentError(f"No database volume found for identifier={self.identifier}")
        return await volume.snapshot(label, backup_id)

    async def get_usage(self) -> VolumeUsage | None:
        return None

    async def provision_volume(
        self,
        name: str,
        size_bytes: int,
        qos: VolumeQosProfile | None = None,
    ) -> Volume:
        return await self._backend.provision_volume(name=name, size_bytes=size_bytes, qos=qos)


@dataclass
class LvmSnapshot(Snapshot):
    source_identifier: Identifier | None
    _backend: "LvmBackend"

    async def delete(self) -> None:
        await delete_snapshot(self.details.namespace, self.details.name)

    async def restore_to(self, target_volume: Volume, database_size: int) -> None:
        if not isinstance(target_volume, LvmVolume):
            raise VelaDeploymentError("Snapshot restore requires an LvmVolume target")
        if self.source_identifier is None:
            raise VelaDeploymentError("Cannot restore snapshot without source identifier context")
        await self._backend._restore_volume_from_snapshot(
            source_identifier=self.source_identifier,
            target_identifier=target_volume.identifier,
            snapshot_ref=self.snapshot_ref,
            database_size=database_size,
        )

    async def clone_to(self, target_volume: Volume, new_size: int) -> None:
        if not isinstance(target_volume, LvmVolume):
            raise VelaDeploymentError("Snapshot clone requires an LvmVolume target")
        if self.source_identifier is None:
            raise VelaDeploymentError("Cannot clone snapshot without source identifier context")
        await self._backend._clone_volume_from_snapshot(
            source_identifier=self.source_identifier,
            target_identifier=target_volume.identifier,
            new_size=new_size,
        )


class LvmBackend(StorageBackend):
    name = "lvm"

    def __init__(self, settings: Settings):
        self.settings = settings
        self._storage_class_validated = False

    def get_capabilities(self) -> StorageCapabilitiesPublic:
        warnings: list[str] = []
        if self.settings.storage_default_class == "simplyblock-csi-sc":
            warnings.append(
                "Using TopoLVM default StorageClass fallback 'topolvm-provisioner' because "
                "vela_storage_default_class is still set to simplyblock default."
            )
        if self.settings.storage_snapshot_class == "simplyblock-csi-snapshotclass":
            warnings.append(
                "Using TopoLVM default VolumeSnapshotClass fallback 'topolvm-snapshotclass' because "
                "vela_storage_snapshot_class is still set to simplyblock default."
            )
        warnings.append("LVM backend currently reports no runtime QoS controls and no usage telemetry.")
        return StorageCapabilitiesPublic(
            backend="lvm",
            capabilities=_CAPABILITIES,
            storage_class=self._effective_storage_class(),
            snapshot_class=self._effective_snapshot_class(),
            qos_policy=self.settings.storage_qos_policy,
            warnings=warnings,
        )

    def resolve_storage_class(self) -> str:
        return self._effective_storage_class()

    def resolve_snapshot_class(self) -> str:
        return self._effective_snapshot_class()

    async def provision_volume(
        self,
        name: str,
        size_bytes: int,
        qos: VolumeQosProfile | None = None,
    ) -> Volume:
        await self._ensure_topolvm_storage_class()
        self.validate_qos_profile(qos or VolumeQosProfile())
        identifier = ULID()
        namespace, autoscaler_vm_name = get_autoscaler_vm_identity(identifier)
        pvc_name = f"{autoscaler_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        return LvmVolume(
            namespace=namespace,
            pvc_name=pvc_name,
            storage_class=self.resolve_storage_class(),
            size_bytes=size_bytes,
            identifier=identifier,
            _backend=self,
        )

    async def provision_volume_group(
        self,
        group_id: Identifier,
        group_name: str,
        qos: VolumeQosProfile | None = None,
    ) -> VolumeGroup:
        await self._ensure_topolvm_storage_class()
        self.validate_qos_profile(qos or VolumeQosProfile())
        return LvmVolumeGroup(name=group_name, identifier=group_id, _backend=self)

    async def lookup_volume(self, volume_id: Identifier) -> Volume | None:
        await self._ensure_topolvm_storage_class()
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
        return LvmVolume(
            namespace=namespace,
            pvc_name=pvc_name,
            storage_class=self.resolve_storage_class(),
            size_bytes=size_bytes,
            identifier=volume_id,
            _backend=self,
        )

    async def lookup_volume_group(self, group_id: Identifier) -> VolumeGroup | None:
        await self._ensure_topolvm_storage_class()
        return LvmVolumeGroup(name=str(group_id), identifier=group_id, _backend=self)

    async def lookup_snapshot(self, snapshot_ref: SnapshotRef) -> Snapshot | None:
        snapshot = await read_snapshot(snapshot_ref.namespace, snapshot_ref.name)
        if snapshot is None:
            return None
        status = snapshot.get("status") or {}
        content_name_payload = status.get("boundVolumeSnapshotContentName")
        details = SnapshotDetails(
            name=snapshot_ref.name,
            namespace=snapshot_ref.namespace,
            content_name=content_name_payload if isinstance(content_name_payload, str) else snapshot_ref.content_name,
            size_bytes=quantity_to_bytes(status.get("restoreSize")),
        )
        resolved_ref = SnapshotRef(name=details.name, namespace=details.namespace, content_name=details.content_name)
        return LvmSnapshot(details=details, snapshot_ref=resolved_ref, source_identifier=None, _backend=self)

    async def get_branch_volume_usage(
        self,
        identifier: Identifier,
        *,
        volume_type: Literal["database", "storage", "wal"] = "database",
    ) -> VolumeUsage | None:
        _ = (identifier, volume_type)
        return None

    async def clone_branch_database_volume(
        self,
        *,
        source_identifier: Identifier,
        target_identifier: Identifier,
        database_size: int,
        pitr_enabled: bool = False,
    ) -> None:
        await clone_branch_database_volume(
            source_branch_id=source_identifier,
            target_branch_id=target_identifier,
            snapshot_class=self.resolve_snapshot_class(),
            storage_class_name=self.resolve_storage_class(),
            snapshot_timeout_seconds=_SNAPSHOT_TIMEOUT_SECONDS,
            snapshot_poll_interval_seconds=_SNAPSHOT_POLL_INTERVAL_SECONDS,
            pvc_timeout_seconds=_PVC_TIMEOUT_SECONDS,
            pvc_poll_interval_seconds=_PVC_POLL_INTERVAL_SECONDS,
            database_size=database_size,
            pitr_enabled=pitr_enabled,
        )

    async def restore_branch_database_volume_from_snapshot(
        self,
        *,
        source_identifier: Identifier,
        target_identifier: Identifier,
        snapshot_ref: SnapshotRef,
        database_size: int,
    ) -> None:
        await restore_branch_database_volume_from_snapshot(
            source_branch_id=source_identifier,
            target_branch_id=target_identifier,
            snapshot_namespace=snapshot_ref.namespace,
            snapshot_name=snapshot_ref.name,
            snapshot_content_name=snapshot_ref.content_name,
            snapshot_class=self.resolve_snapshot_class(),
            storage_class_name=self.resolve_storage_class(),
            database_size=database_size,
            snapshot_timeout_seconds=_SNAPSHOT_TIMEOUT_SECONDS,
            snapshot_poll_interval_seconds=_SNAPSHOT_POLL_INTERVAL_SECONDS,
            pvc_timeout_seconds=_PVC_TIMEOUT_SECONDS,
            pvc_poll_interval_seconds=_PVC_POLL_INTERVAL_SECONDS,
        )

    def validate_qos_profile(self, qos: VolumeQosProfile) -> None:
        requested_fields = [
            field_name
            for field_name in (
                "max_read_iops",
                "max_write_iops",
                "max_read_write_iops",
                "max_read_mibps",
                "max_write_mibps",
                "max_read_write_mibps",
            )
            if getattr(qos, field_name) is not None
        ]
        if requested_fields and self.settings.storage_qos_policy == "strict":
            raise VelaDeploymentError(
                f"lvm backend does not support QoS controls; unsupported fields: {', '.join(requested_fields)}"
            )

    def validate_capabilities_for_operation(self, operation: str, params: dict[str, object] | None = None) -> None:
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
        snapshot_name = self._build_snapshot_name(label=label or "backup", backup_id=backup_id or ULID())
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
        await self.clone_branch_database_volume(
            source_identifier=source_identifier,
            target_identifier=target_identifier,
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
        await self.restore_branch_database_volume_from_snapshot(
            source_identifier=source_identifier,
            target_identifier=target_identifier,
            snapshot_ref=snapshot_ref,
            database_size=database_size,
        )

    def _build_snapshot_name(self, *, label: str, backup_id: ULID) -> str:
        clean_label = re.sub(r"[^a-z0-9-]", "-", label.lower())
        clean_label = re.sub(r"-+", "-", clean_label).strip("-") or "backup"
        clean_backup = re.sub(r"[^a-z0-9-]", "-", str(backup_id).lower())
        clean_backup = re.sub(r"-+", "-", clean_backup).strip("-") or datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        return f"{clean_label}-{clean_backup}"[:_K8S_NAME_MAX_LENGTH].strip("-")

    def _is_not_found_error(self, exc: VelaKubernetesError) -> bool:
        return "not found" in str(exc).lower()

    def _effective_storage_class(self) -> str:
        if self.settings.storage_default_class == "simplyblock-csi-sc":
            return _DEFAULT_TOPO_LVM_STORAGE_CLASS
        return self.settings.storage_default_class

    def _effective_snapshot_class(self) -> str:
        if self.settings.storage_snapshot_class == "simplyblock-csi-snapshotclass":
            return _DEFAULT_TOPO_LVM_SNAPSHOT_CLASS
        return self.settings.storage_snapshot_class

    async def _ensure_topolvm_storage_class(self) -> None:
        if self._storage_class_validated:
            return
        storage_class_name = self._effective_storage_class()
        storage_class = await kube_service.get_storage_class(storage_class_name)
        provisioner = str(getattr(storage_class, "provisioner", "") or "").lower()
        if not any(marker in provisioner for marker in _TOPO_LVM_PROVISIONER_MARKERS):
            raise VelaDeploymentError(
                f"LVM backend expects a TopoLVM StorageClass. "
                f"Configured class {storage_class_name!r} uses provisioner {provisioner!r}. "
                "Set vela_storage_default_class to a TopoLVM class (for example 'topolvm-provisioner')."
            )
        self._storage_class_validated = True
