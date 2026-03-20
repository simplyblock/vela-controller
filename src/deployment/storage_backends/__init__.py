from typing import Literal
from uuid import UUID

from .base import (
    Identifier,
    Snapshot,
    SnapshotRef,
    StorageBackend,
    StorageBackendName,
    StorageCapabilitiesPublic,
    StorageTelemetryStatus,
    StorageQosPolicy,
    Volume,
    VolumeCapabilities,
    VolumeGroup,
    VolumeQosProfile,
    VolumeSpec,
    VolumeUsage,
)
from .lvm import LvmBackend
from .simplyblock import SimplyblockBackend
from ..settings import get_settings
from ...exceptions import VelaDeploymentError


class PlaceholderBackend(StorageBackend):
    def __init__(self, name: StorageBackendName):
        self.name = name

    def get_capabilities(self) -> StorageCapabilitiesPublic:
        settings = get_settings()
        warning = f"Storage backend '{self.name}' abstraction is not implemented yet; reporting conservative capabilities."
        return StorageCapabilitiesPublic(
            backend=self.name,
            capabilities=VolumeCapabilities(
                supports_dynamic_provisioning=False,
                supports_storage_class_per_branch=False,
                supports_storage_class_shared=False,
                supports_topology_awareness=False,
                supports_encrypted_volumes=False,
                supports_vm_live_migration=False,
                supports_usage_qos_metrics=False,
                supports_qos_read_write_split=False,
                supports_usage_storage_metrics=False,
                supports_file_storage_volume=False,
                supports_pitr_wal_volume=False,
                supports_snapshot_content_rebind=False,
                supports_clone_without_snapshot=False,
                supports_fast_clone=False,
                supports_backup_snapshot_labels=False,
                supports_restore_size_discovery=False,
                supports_volume_groups=False,
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
                supports_snapshots=False,
                supports_snapshot_restore=False,
                supports_volume_clone_cross_namespace=False,
                supports_volume_expansion=False,
                supports_volume_expansion_online=False,
                supports_volume_relocation=False,
            ),
            storage_class=settings.storage_default_class,
            snapshot_class=settings.storage_snapshot_class,
            qos_policy=settings.storage_qos_policy,
            warnings=[warning],
        )

    def resolve_storage_class(self) -> str:
        return get_settings().storage_default_class

    def resolve_snapshot_class(self) -> str:
        return get_settings().storage_snapshot_class

    async def provision_volume(
        self,
        name: str,
        size_bytes: int,
        qos: VolumeQosProfile | None = None,
    ) -> Volume:
        self.validate_capabilities_for_operation("volume_provision")
        raise self._unsupported("volume provisioning")

    async def provision_volume_group(
        self,
        group_id: Identifier,
        group_name: str,
        qos: VolumeQosProfile | None = None,
    ) -> VolumeGroup:
        self.validate_capabilities_for_operation("volume_group_provision")
        raise self._unsupported("volume group provisioning")

    async def lookup_volume(self, volume_id: Identifier) -> Volume | None:
        self.validate_capabilities_for_operation("volume_lookup")
        raise self._unsupported("volume lookup")

    async def lookup_volume_group(self, group_id: Identifier) -> VolumeGroup | None:
        self.validate_capabilities_for_operation("volume_group_lookup")
        raise self._unsupported("volume group lookup")

    async def lookup_snapshot(self, snapshot_ref: SnapshotRef) -> Snapshot | None:
        self.validate_capabilities_for_operation("snapshot_lookup")
        raise self._unsupported("snapshot lookup")

    async def get_branch_volume_usage(
        self,
        identifier: Identifier,
        *,
        volume_type: Literal["database", "storage", "wal"] = "database",
    ) -> VolumeUsage | None:
        _ = (identifier, volume_type)
        self.validate_capabilities_for_operation("volume_usage_storage_metrics")
        return None

    async def get_snapshot_used_size(self, snapshot_ids: list[UUID]) -> int | None:
        _ = snapshot_ids
        self.validate_capabilities_for_operation("usage_storage_metrics")
        return None

    def get_telemetry_status(self) -> StorageTelemetryStatus:
        capabilities = self.get_capabilities().capabilities
        return StorageTelemetryStatus(
            volume_metrics_available=bool(capabilities.supports_usage_storage_metrics),
            snapshot_metrics_available=bool(capabilities.supports_usage_storage_metrics),
        )

    async def clone_branch_database_volume(
        self,
        *,
        source_identifier: Identifier,
        target_identifier: Identifier,
        database_size: int,
        pitr_enabled: bool = False,
    ) -> None:
        _ = (source_identifier, target_identifier, database_size, pitr_enabled)
        self.validate_capabilities_for_operation("snapshot_restore")
        raise self._unsupported("branch database volume clone")

    async def restore_branch_database_volume_from_snapshot(
        self,
        *,
        source_identifier: Identifier,
        target_identifier: Identifier,
        snapshot_ref: SnapshotRef,
        database_size: int,
    ) -> None:
        _ = (source_identifier, target_identifier, snapshot_ref, database_size)
        self.validate_capabilities_for_operation("snapshot_restore")
        raise self._unsupported("branch database volume restore")

    def validate_qos_profile(self, qos: VolumeQosProfile) -> None:
        return None

    def validate_capabilities_for_operation(self, operation: str, params: dict[str, object] | None = None) -> None:
        capabilities = self.get_capabilities().capabilities
        checks = {
            "volume_provision": capabilities.supports_dynamic_provisioning,
            "volume_group_provision": capabilities.supports_volume_groups,
            "volume_lookup": capabilities.supports_dynamic_provisioning,
            "volume_group_lookup": capabilities.supports_volume_groups,
            "snapshot_lookup": capabilities.supports_snapshots,
            "snapshots": capabilities.supports_snapshots,
            "snapshot_restore": capabilities.supports_snapshot_restore,
            "volume_expansion": capabilities.supports_volume_expansion,
            "volume_relocation": capabilities.supports_volume_relocation,
            "volume_iops_update": capabilities.supports_volume_iops_update,
            "volume_group_iops_update": capabilities.supports_volume_group_iops_update,
            "volume_group_throughput_update": capabilities.supports_volume_group_throughput_update,
            "volume_usage_storage_metrics": capabilities.supports_volume_usage_storage_metrics,
            "usage_storage_metrics": capabilities.supports_usage_storage_metrics,
        }
        supported = checks.get(operation)
        if supported is False:
            raise VelaDeploymentError(
                f"Operation {operation!r} is not supported by backend {self.name!r} "
                "(capabilities endpoint reports it as unavailable)"
            )

    def _unsupported(self, capability: str) -> VelaDeploymentError:
        return VelaDeploymentError(
            f"Storage backend '{self.name}' does not support {capability}; "
            "select a backend with the required capability or adjust storage settings."
        )


def get_storage_backend() -> StorageBackend:
    settings = get_settings()
    backend = settings.storage_backend
    if backend == "simplyblock":
        return SimplyblockBackend(settings)
    if backend == "lvm":
        return LvmBackend(settings)
    raise VelaDeploymentError(
        f"Unsupported storage backend {backend!r}. Supported backends are: simplyblock, lvm."
    )


__all__ = [
    "Identifier",
    "Snapshot",
    "SnapshotRef",
    "StorageBackend",
    "StorageBackendName",
    "StorageCapabilitiesPublic",
    "StorageTelemetryStatus",
    "StorageQosPolicy",
    "Volume",
    "VolumeCapabilities",
    "VolumeGroup",
    "VolumeQosProfile",
    "VolumeSpec",
    "VolumeUsage",
    "get_storage_backend",
]
