# PVC Clone and Restore Across Namespaces

1. Capture or load a source snapshot.
2. Extract snapshot metadata from `VolumeSnapshotContent` (`snapshotHandle`, `driver`, class).
3. Create a target-side `VolumeSnapshotContent` that points to the same backend `snapshotHandle`.
4. Create a target-side `VolumeSnapshot` bound to that content.
5. Create a target PVC whose `dataSource` is the target `VolumeSnapshot`.

Because `VolumeSnapshotContent` is cluster-scoped and `VolumeSnapshot` is namespaced, this pattern enables cross-namespace restore/clone.
