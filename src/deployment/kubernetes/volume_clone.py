import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ..._util import Identifier
from ...exceptions import VelaKubernetesError
from .. import DATABASE_PVC_SUFFIX, deployment_namespace, kube_service
from ..settings import settings as deployment_settings
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
        pvc_name = f"{deployment_settings.deployment_release_name}{DATABASE_PVC_SUFFIX}"
        source_ns = deployment_namespace(self.source_branch_id)
        target_ns = deployment_namespace(self.target_branch_id)
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

        content_name = (snapshot.get("status") or {}).get("boundVolumeSnapshotContentName")
        if not content_name:
            raise VelaKubernetesError(
                f"VolumeSnapshot {self.ids.source_namespace}/{self.ids.source_snapshot} missing content reference"
            )

        source_content = await read_snapshot_content(content_name)
        if source_content is None:
            raise VelaKubernetesError(f"VolumeSnapshotContent {content_name} not found for source snapshot clone")

        snapshot_handle = (source_content.get("status") or {}).get("snapshotHandle")
        if not snapshot_handle:
            raise VelaKubernetesError(f"VolumeSnapshotContent {content_name} missing snapshotHandle")

        driver = (source_content.get("spec") or {}).get("driver")
        if not driver:
            raise VelaKubernetesError(f"VolumeSnapshotContent {content_name} missing driver")

        resolved_snapshot_class = (
            (source_content.get("spec") or {}).get("volumeSnapshotClassName")
            or (snapshot.get("spec") or {}).get("volumeSnapshotClassName")
            or self.snapshot_class
        )

        return SnapshotMaterial(
            driver=driver,
            handle=snapshot_handle,
            snapshot_class=resolved_snapshot_class,
        )

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
        annotations["meta.helm.sh/release-name"] = deployment_settings.deployment_release_name
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
