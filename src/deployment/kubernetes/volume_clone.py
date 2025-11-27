import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..._util import Identifier
from ...exceptions import VelaKubernetesError
from .. import AUTOSCALER_PVC_SUFFIX, get_autoscaler_vm_identity, kube_service
from ..settings import get_settings
from .pvc import (
    build_pvc_manifest_from_existing,
    create_pvc,
    delete_pvc,
    wait_for_pvc_absent,
    wait_for_pvc_bound,
)
from .snapshot import (
    create_snapshot_content_from_handle,
    create_snapshot_from_content,
    create_snapshot_from_pvc,
    delete_snapshot,
    delete_snapshot_content,
    ensure_snapshot_absent,
    ensure_snapshot_content_absent,
    read_snapshot_content,
    wait_snapshot_ready,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CloneTimeouts:
    snapshot_ready: float
    snapshot_poll: float
    pvc_ready: float
    pvc_poll: float


@dataclass(frozen=True)
class CloneIdentifiers:
    pvc: str
    source_namespace: str
    target_namespace: str
    source_snapshot: str
    target_snapshot: str
    snapshot_content: str


@dataclass(frozen=True)
class SnapshotMaterial:
    driver: str
    handle: str
    snapshot_class: str


async def _extract_snapshot_material(
    *,
    namespace: str,
    snapshot: dict[str, Any],
    explicit_content_name: str | None,
    default_snapshot_class: str,
) -> tuple[SnapshotMaterial, str]:
    status = snapshot.get("status") or {}
    content_name = explicit_content_name or status.get("boundVolumeSnapshotContentName")
    if not content_name:
        raise VelaKubernetesError(
            f"VolumeSnapshot {namespace}/{snapshot.get('metadata', {}).get('name')} missing content reference"
        )

    source_content = await read_snapshot_content(content_name)
    if source_content is None:
        raise VelaKubernetesError(f"VolumeSnapshotContent {content_name} not found for snapshot restoration")

    snapshot_handle = (source_content.get("status") or {}).get("snapshotHandle")
    if not snapshot_handle:
        raise VelaKubernetesError(f"VolumeSnapshotContent {content_name} missing snapshotHandle")

    driver = (source_content.get("spec") or {}).get("driver")
    if not driver:
        raise VelaKubernetesError(f"VolumeSnapshotContent {content_name} missing driver")

    resolved_snapshot_class = (
        (source_content.get("spec") or {}).get("volumeSnapshotClassName")
        or (snapshot.get("spec") or {}).get("volumeSnapshotClassName")
        or default_snapshot_class
    )
    if not resolved_snapshot_class:
        raise VelaKubernetesError(f"Unable to resolve snapshot class for VolumeSnapshotContent {content_name}")

    material = SnapshotMaterial(
        driver=driver,
        handle=snapshot_handle,
        snapshot_class=resolved_snapshot_class,
    )
    return material, content_name


@dataclass
class _VolumeCloneOperation:
    source_branch_id: Identifier
    target_branch_id: Identifier
    snapshot_class: str
    timeouts: CloneTimeouts
    storage_class_name: str
    ids: CloneIdentifiers = field(init=False)
    created_source_snapshot: bool = field(default=False, init=False)
    created_target_snapshot: bool = field(default=False, init=False)
    created_content: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        source_ns, source_vm_name = get_autoscaler_vm_identity(self.source_branch_id)
        target_ns, target_vm_name = get_autoscaler_vm_identity(self.target_branch_id)
        pvc_name = f"{source_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        target_pvc_name = f"{target_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        if target_pvc_name != pvc_name:
            raise VelaKubernetesError(
                f"Autoscaler PVC name mismatch between source ({pvc_name}) and target ({target_pvc_name})"
            )
        timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        source_snapshot = f"{str(self.source_branch_id).lower()}-snapshot-{timestamp}"[:63]
        target_snapshot = f"{str(self.target_branch_id).lower()}-snapshot-{timestamp}"[:63]
        snapshot_content = f"snapcontent-crossns-{str(self.target_branch_id).lower()}-{timestamp}"[:63]

        object.__setattr__(
            self,
            "ids",
            CloneIdentifiers(
                pvc=pvc_name,
                source_namespace=source_ns,
                target_namespace=target_ns,
                source_snapshot=source_snapshot,
                target_snapshot=target_snapshot,
                snapshot_content=snapshot_content,
            ),
        )

    async def run(self) -> None:
        """Perform the full clone flow end-to-end for the configured branches."""
        await kube_service.ensure_namespace(self.ids.target_namespace)

        async with self._cleanup_on_failure():
            await self._clear_previous_artifacts()
            snapshot_material = await self._capture_source_snapshot()
            logger.info(
                "Captured source snapshot %s/%s for branch clone %s -> %s",
                self.ids.source_namespace,
                self.ids.source_snapshot,
                self.source_branch_id,
                self.target_branch_id,
            )
            await self._materialize_target_snapshot(snapshot_material)
            logger.info(
                "Materialized target snapshot %s/%s using content %s",
                self.ids.target_namespace,
                self.ids.target_snapshot,
                self.ids.snapshot_content,
            )
            await self._create_target_pvc()
            logger.info(
                "Successfully cloned PVC %s from %s to %s for branch %s",
                self.ids.pvc,
                self.ids.source_namespace,
                self.ids.target_namespace,
                self.target_branch_id,
            )

    @contextlib.asynccontextmanager
    async def _cleanup_on_failure(self):
        """Ensure temporary artefacts are deleted if any stage raises."""
        try:
            yield
        except Exception:
            await self._cleanup_created_resources()
            raise

    async def _clear_previous_artifacts(self) -> None:
        """Remove any lingering snapshots or contents from previous attempts."""
        await ensure_snapshot_absent(
            self.ids.source_namespace,
            self.ids.source_snapshot,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )
        await ensure_snapshot_absent(
            self.ids.target_namespace,
            self.ids.target_snapshot,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )
        await ensure_snapshot_content_absent(
            self.ids.snapshot_content,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )

    async def _capture_source_snapshot(self) -> SnapshotMaterial:
        """Snapshot the source PVC and return the metadata required to recreate it elsewhere."""
        await create_snapshot_from_pvc(
            namespace=self.ids.source_namespace,
            name=self.ids.source_snapshot,
            snapshot_class=self.snapshot_class,
            pvc_name=self.ids.pvc,
        )
        self.created_source_snapshot = True

        snapshot = await wait_snapshot_ready(
            self.ids.source_namespace,
            self.ids.source_snapshot,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )

        material, _ = await _extract_snapshot_material(
            namespace=self.ids.source_namespace,
            snapshot=snapshot,
            explicit_content_name=None,
            default_snapshot_class=self.snapshot_class,
        )
        return material

    async def _materialize_target_snapshot(self, material: SnapshotMaterial) -> None:
        """Import the captured snapshot into the target namespace."""
        await create_snapshot_content_from_handle(
            name=self.ids.snapshot_content,
            driver=material.driver,
            snapshot_handle=material.handle,
            snapshot_class=material.snapshot_class,
            snapshot_namespace=self.ids.target_namespace,
            snapshot_name=self.ids.target_snapshot,
        )
        self.created_content = True

        await create_snapshot_from_content(
            namespace=self.ids.target_namespace,
            name=self.ids.target_snapshot,
            snapshot_class=material.snapshot_class,
            content_name=self.ids.snapshot_content,
        )
        self.created_target_snapshot = True

        await wait_snapshot_ready(
            self.ids.target_namespace,
            self.ids.target_snapshot,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )

    async def _create_target_pvc(self) -> None:
        """Create or replace the target PVC from the cloned snapshot."""
        namespace = self.ids.target_namespace
        pvc_name = self.ids.pvc
        snapshot_name = self.ids.target_snapshot

        source_pvc = await kube_service.get_persistent_volume_claim(self.ids.source_namespace, pvc_name)
        new_manifest = build_pvc_manifest_from_existing(
            source_pvc,
            branch_id=self.target_branch_id,
            volume_snapshot_name=snapshot_name,
        )
        new_manifest.spec.storage_class_name = self.storage_class_name
        if hasattr(new_manifest.spec, "storageClassName"):
            new_manifest.spec.storageClassName = self.storage_class_name
        annotations = dict(getattr(new_manifest.metadata, "annotations", {}) or {})
        annotations["meta.helm.sh/release-name"] = get_settings().deployment_release_name
        annotations["meta.helm.sh/release-namespace"] = namespace
        new_manifest.metadata.annotations = annotations

        await delete_pvc(namespace, pvc_name)
        await wait_for_pvc_absent(
            namespace,
            pvc_name,
            timeout=self.timeouts.pvc_ready,
            poll_interval=self.timeouts.pvc_poll,
        )

        await create_pvc(namespace, new_manifest)
        await wait_for_pvc_bound(
            namespace,
            pvc_name,
            timeout=self.timeouts.pvc_ready,
            poll_interval=self.timeouts.pvc_poll,
        )

    async def _cleanup_created_resources(self) -> None:
        """Best-effort removal of snapshots and snapshot content created during this run."""
        if self.created_target_snapshot:
            with contextlib.suppress(VelaKubernetesError):
                await delete_snapshot(self.ids.target_namespace, self.ids.target_snapshot)
        if self.created_content:
            with contextlib.suppress(VelaKubernetesError):
                await delete_snapshot_content(self.ids.snapshot_content)
        if self.created_source_snapshot:
            with contextlib.suppress(VelaKubernetesError):
                await delete_snapshot(self.ids.source_namespace, self.ids.source_snapshot)


@dataclass
class _SnapshotRestoreOperation:
    source_branch_id: Identifier
    target_branch_id: Identifier
    snapshot_namespace: str
    snapshot_name: str
    snapshot_content_name: str | None
    snapshot_class: str
    storage_class_name: str
    timeouts: CloneTimeouts
    ids: CloneIdentifiers = field(init=False)
    created_target_snapshot: bool = field(default=False, init=False)
    created_content: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        source_ns = self.snapshot_namespace
        _, source_vm_name = get_autoscaler_vm_identity(self.source_branch_id)
        target_ns, target_vm_name = get_autoscaler_vm_identity(self.target_branch_id)
        pvc_name = f"{source_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        target_pvc_name = f"{target_vm_name}{AUTOSCALER_PVC_SUFFIX}"
        if target_pvc_name != pvc_name:
            raise VelaKubernetesError(
                f"Autoscaler PVC name mismatch between source ({pvc_name}) and target ({target_pvc_name})"
            )
        timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        target_snapshot = f"{str(self.target_branch_id).lower()}-restore-{timestamp}"[:63]
        snapshot_content = f"snapcontent-restore-{str(self.target_branch_id).lower()}-{timestamp}"[:63]

        object.__setattr__(
            self,
            "ids",
            CloneIdentifiers(
                pvc=pvc_name,
                source_namespace=source_ns,
                target_namespace=target_ns,
                source_snapshot=self.snapshot_name,
                target_snapshot=target_snapshot,
                snapshot_content=snapshot_content,
            ),
        )

    async def run(self) -> None:
        await kube_service.ensure_namespace(self.ids.target_namespace)

        async with self._cleanup_on_failure():
            await self._clear_previous_artifacts()
            material = await self._load_snapshot_material()
            logger.info(
                "Using snapshot %s/%s for branch restore %s -> %s",
                self.snapshot_namespace,
                self.snapshot_name,
                self.source_branch_id,
                self.target_branch_id,
            )
            await self._materialize_target_snapshot(material)
            logger.info(
                "Materialized restore snapshot %s/%s using content %s",
                self.ids.target_namespace,
                self.ids.target_snapshot,
                self.ids.snapshot_content,
            )
            await self._create_target_pvc()
            logger.info(
                "Restored PVC %s into %s for branch %s",
                self.ids.pvc,
                self.ids.target_namespace,
                self.target_branch_id,
            )

    async def _clear_previous_artifacts(self) -> None:
        await ensure_snapshot_absent(
            self.ids.target_namespace,
            self.ids.target_snapshot,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )
        await ensure_snapshot_content_absent(
            self.ids.snapshot_content,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )

    async def _load_snapshot_material(self) -> SnapshotMaterial:
        snapshot = await wait_snapshot_ready(
            self.snapshot_namespace,
            self.snapshot_name,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )
        material, _ = await _extract_snapshot_material(
            namespace=self.snapshot_namespace,
            snapshot=snapshot,
            explicit_content_name=self.snapshot_content_name,
            default_snapshot_class=self.snapshot_class,
        )
        return material

    async def _materialize_target_snapshot(self, material: SnapshotMaterial) -> None:
        await create_snapshot_content_from_handle(
            name=self.ids.snapshot_content,
            driver=material.driver,
            snapshot_handle=material.handle,
            snapshot_class=material.snapshot_class,
            snapshot_namespace=self.ids.target_namespace,
            snapshot_name=self.ids.target_snapshot,
        )
        self.created_content = True

        await create_snapshot_from_content(
            namespace=self.ids.target_namespace,
            name=self.ids.target_snapshot,
            snapshot_class=material.snapshot_class,
            content_name=self.ids.snapshot_content,
        )
        self.created_target_snapshot = True

        await wait_snapshot_ready(
            self.ids.target_namespace,
            self.ids.target_snapshot,
            timeout=self.timeouts.snapshot_ready,
            poll_interval=self.timeouts.snapshot_poll,
        )

    async def _create_target_pvc(self) -> None:
        source_pvc = await kube_service.get_persistent_volume_claim(self.ids.source_namespace, self.ids.pvc)
        new_manifest = build_pvc_manifest_from_existing(
            source_pvc,
            branch_id=self.target_branch_id,
            volume_snapshot_name=self.ids.target_snapshot,
        )
        new_manifest.spec.storage_class_name = self.storage_class_name
        if hasattr(new_manifest.spec, "storageClassName"):
            new_manifest.spec.storageClassName = self.storage_class_name
        annotations = dict(getattr(new_manifest.metadata, "annotations", {}) or {})
        annotations["meta.helm.sh/release-name"] = get_settings().deployment_release_name
        annotations["meta.helm.sh/release-namespace"] = self.ids.target_namespace
        new_manifest.metadata.annotations = annotations

        await delete_pvc(self.ids.target_namespace, self.ids.pvc)
        await wait_for_pvc_absent(
            self.ids.target_namespace,
            self.ids.pvc,
            timeout=self.timeouts.pvc_ready,
            poll_interval=self.timeouts.pvc_poll,
        )

        await create_pvc(self.ids.target_namespace, new_manifest)
        await wait_for_pvc_bound(
            self.ids.target_namespace,
            self.ids.pvc,
            timeout=self.timeouts.pvc_ready,
            poll_interval=self.timeouts.pvc_poll,
        )

    @contextlib.asynccontextmanager
    async def _cleanup_on_failure(self):
        try:
            yield
        except Exception:
            if self.created_target_snapshot:
                with contextlib.suppress(VelaKubernetesError):
                    await delete_snapshot(self.ids.target_namespace, self.ids.target_snapshot)
            if self.created_content:
                with contextlib.suppress(VelaKubernetesError):
                    await delete_snapshot_content(self.ids.snapshot_content)
            raise


async def clone_branch_database_volume(
    *,
    source_branch_id: Identifier,
    target_branch_id: Identifier,
    snapshot_class: str,
    storage_class_name: str,
    snapshot_timeout_seconds: float,
    snapshot_poll_interval_seconds: float,
    pvc_timeout_seconds: float,
    pvc_poll_interval_seconds: float,
) -> None:
    """
    Clone the database volume from one branch to another using CSI snapshots.
    """
    operation = _VolumeCloneOperation(
        source_branch_id=source_branch_id,
        target_branch_id=target_branch_id,
        snapshot_class=snapshot_class,
        storage_class_name=storage_class_name,
        timeouts=CloneTimeouts(
            snapshot_ready=snapshot_timeout_seconds,
            snapshot_poll=snapshot_poll_interval_seconds,
            pvc_ready=pvc_timeout_seconds,
            pvc_poll=pvc_poll_interval_seconds,
        ),
    )
    await operation.run()


async def restore_branch_database_volume_from_snapshot(
    *,
    source_branch_id: Identifier,
    target_branch_id: Identifier,
    snapshot_namespace: str,
    snapshot_name: str,
    snapshot_content_name: str | None,
    snapshot_class: str,
    storage_class_name: str,
    snapshot_timeout_seconds: float,
    snapshot_poll_interval_seconds: float,
    pvc_timeout_seconds: float,
    pvc_poll_interval_seconds: float,
) -> None:
    """
    Restore the database volume for a branch from an existing VolumeSnapshot.
    """
    operation = _SnapshotRestoreOperation(
        source_branch_id=source_branch_id,
        target_branch_id=target_branch_id,
        snapshot_namespace=snapshot_namespace,
        snapshot_name=snapshot_name,
        snapshot_content_name=snapshot_content_name,
        snapshot_class=snapshot_class,
        storage_class_name=storage_class_name,
        timeouts=CloneTimeouts(
            snapshot_ready=snapshot_timeout_seconds,
            snapshot_poll=snapshot_poll_interval_seconds,
            pvc_ready=pvc_timeout_seconds,
            pvc_poll=pvc_poll_interval_seconds,
        ),
    )
    await operation.run()
