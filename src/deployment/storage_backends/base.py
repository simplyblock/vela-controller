from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from ulid import ULID

StorageBackendName = Literal["simplyblock", "lvm"]
StorageQosPolicy = Literal["strict", "best_effort"]
Identifier = ULID


class VolumeCapabilities(BaseModel):
    # Backend supports StorageClass-based dynamic provisioning.
    supports_dynamic_provisioning: bool
    # Backend supports per-branch StorageClass strategy.
    supports_storage_class_per_branch: bool
    # Backend supports shared StorageClass strategy across branches.
    supports_storage_class_shared: bool
    # Backend supports topology/placement-aware provisioning.
    supports_topology_awareness: bool
    # Backend supports encrypted volumes natively.
    supports_encrypted_volumes: bool
    # Backend supports live migration characteristics without storage relocation interruption.
    supports_vm_live_migration: bool
    # Backend can report system-level observed IOPS and throughput usage metrics.
    supports_usage_qos_metrics: bool
    # Backend supports distinct read/write QoS limits.
    supports_qos_read_write_split: bool
    # Backend can report system-level observed used-bytes/storage usage metrics.
    supports_usage_storage_metrics: bool
    # Backend supports provisioning the optional file storage volume.
    supports_file_storage_volume: bool
    # Backend supports dedicated WAL volume for PITR paths.
    supports_pitr_wal_volume: bool
    # Backend supports snapshot content rebind/import workflows.
    supports_snapshot_content_rebind: bool
    # Backend supports direct clone without explicit snapshot object.
    supports_clone_without_snapshot: bool
    # Backend supports fast/native clone semantics (e.g., COW metadata clone).
    supports_fast_clone: bool
    # Backend supports custom labels/metadata on backup snapshots.
    supports_backup_snapshot_labels: bool
    # Backend can discover required restore size from snapshot metadata.
    supports_restore_size_discovery: bool

    # Backend exposes volume groups as first-class entities.
    supports_volume_groups: bool
    # Backend supports consistency-group snapshots across volumes in the volume group.
    supports_consistency_group_snapshots: bool
    # Backend supports IOPS controls per-volume-group and provision time.
    supports_volume_group_iops: bool
    # Backend supports changing per-volume-group IOPS after creation.
    supports_volume_group_iops_update: bool
    # Backend supports throughput controls per-volume-group and provision time.
    supports_volume_group_throughput: bool
    # Backend supports changing per-volume-group throughput after creation.
    supports_volume_group_throughput_update: bool
    # Backend can report observed per-volume-group IOPS and throughput usage metrics.
    supports_volume_group_usage_qos_metrics: bool
    # Backend can report observed per-volume-group used-bytes/storage usage metrics.
    supports_volume_group_usage_storage_metrics: bool

    # Backend supports setting per-volume IOPS at provision time.
    supports_volume_iops: bool
    # Backend supports changing per-volume IOPS after creation.
    supports_volume_iops_update: bool
    # Backend supports setting throughput limits at provision time.
    supports_volume_throughput: bool
    # Backend supports changing throughput limits after creation.
    supports_volume_throughput_update: bool
    # Backend can report observed per-volume IOPS and throughput usage metrics.
    supports_volume_usage_qos_metrics: bool
    # Backend can report observed per-volume used-bytes/storage usage metrics.
    supports_volume_usage_storage_metrics: bool
    # Backend can clone/restore volumes across Kubernetes namespaces.
    supports_volume_clone_cross_namespace: bool
    # Backend supports increasing volume size.
    supports_volume_expansion: bool
    # Backend supports online resize while workload is active.
    supports_volume_expansion_online: bool
    # Backend supports relocating volumes between nodes.
    supports_volume_relocation: bool


class StorageCapabilitiesPublic(BaseModel):
    backend: StorageBackendName
    capabilities: VolumeCapabilities
    storage_class: str
    snapshot_class: str
    qos_policy: StorageQosPolicy
    warnings: list[str] = Field(default_factory=list)


class StorageTelemetryStatus(BaseModel):
    volume_metrics_available: bool
    snapshot_metrics_available: bool


class VolumeQosProfile(BaseModel):
    max_read_iops: int | None = None
    max_write_iops: int | None = None
    max_read_write_iops: int | None = None
    max_read_mibps: int | None = None
    max_write_mibps: int | None = None
    max_read_write_mibps: int | None = None


class SnapshotDetails(BaseModel):
    name: str
    namespace: str
    content_name: str | None = None
    size_bytes: int | None = None


class SnapshotRef(BaseModel):
    name: str
    namespace: str
    content_name: str | None = None


class VolumeSpec(BaseModel):
    size_bytes: int


class VolumeUsage(BaseModel):
    used_bytes: int | None = None
    read_iops: int | None = None
    write_iops: int | None = None
    read_mibps: float | None = None
    write_mibps: float | None = None


@dataclass
class Volume(ABC):
    namespace: str
    pvc_name: str
    storage_class: str
    size_bytes: int

    @abstractmethod
    async def resize(self, new_size_bytes: int) -> None: ...

    @abstractmethod
    async def delete(self) -> None: ...

    @abstractmethod
    async def snapshot(self, label: str, backup_id: Identifier) -> "Snapshot": ...

    @abstractmethod
    async def update_performance(self, qos: "VolumeQosProfile") -> None: ...

    @abstractmethod
    async def usage(self) -> "VolumeUsage | None": ...

    @abstractmethod
    async def relocate(self, target_node: str | None = None) -> None: ...


@dataclass
class VolumeGroup(ABC):
    name: str

    @abstractmethod
    async def delete(self) -> None: ...

    @abstractmethod
    async def update_performance(self, qos: "VolumeQosProfile") -> None: ...

    @abstractmethod
    async def volumes(self) -> list[Volume]: ...

    @abstractmethod
    async def snapshot(self, label: str, backup_id: Identifier) -> "Snapshot": ...

    @abstractmethod
    async def provision_volume(
            self,
            name: str,
            size_bytes: int,
            qos: VolumeQosProfile | None = None,
    ) -> Volume: ...


@dataclass
class Snapshot(ABC):
    details: SnapshotDetails
    snapshot_ref: SnapshotRef

    @abstractmethod
    async def delete(self) -> None: ...

    @abstractmethod
    async def restore_to(self, target_volume: Volume, database_size: int) -> None: ...

    @abstractmethod
    async def clone_to(self, target_volume: Volume, new_size: int) -> None: ...


class StorageBackend(ABC):
    name: StorageBackendName

    @abstractmethod
    def resolve_storage_class(self) -> str: ...

    @abstractmethod
    def resolve_snapshot_class(self) -> str: ...

    @abstractmethod
    async def provision_volume(
            self,
            name: str,
            size_bytes: int,
            qos: VolumeQosProfile | None = None,
    ) -> Volume: ...

    @abstractmethod
    async def provision_volume_group(
            self,
            group_id: Identifier,
            group_name: str,
            qos: VolumeQosProfile | None = None,
    ) -> VolumeGroup: ...

    @abstractmethod
    async def lookup_volume(self, volume_id: Identifier) -> Volume | None: ...

    @abstractmethod
    async def lookup_volume_group(self, group_id: Identifier) -> VolumeGroup | None: ...

    @abstractmethod
    async def lookup_snapshot(self, snapshot_ref: SnapshotRef) -> Snapshot | None: ...

    @abstractmethod
    async def get_branch_volume_usage(
            self,
            identifier: Identifier,
            *,
            volume_type: Literal["database", "storage", "wal"] = "database",
    ) -> VolumeUsage | None: ...

    @abstractmethod
    async def get_snapshot_used_size(self, snapshot_ids: list[UUID]) -> int | None: ...

    @abstractmethod
    def get_telemetry_status(self) -> StorageTelemetryStatus: ...

    @abstractmethod
    async def clone_branch_database_volume(
            self,
            *,
            source_identifier: Identifier,
            target_identifier: Identifier,
            database_size: int,
            pitr_enabled: bool = False,
    ) -> None: ...

    @abstractmethod
    async def restore_branch_database_volume_from_snapshot(
            self,
            *,
            source_identifier: Identifier,
            target_identifier: Identifier,
            snapshot_ref: SnapshotRef,
            database_size: int,
    ) -> None: ...

    @abstractmethod
    def validate_qos_profile(self, qos: VolumeQosProfile) -> None: ...

    @abstractmethod
    def validate_capabilities_for_operation(self, operation: str, params: dict[str, Any] | None = None) -> None: ...

    @abstractmethod
    def get_capabilities(self) -> StorageCapabilitiesPublic: ...
