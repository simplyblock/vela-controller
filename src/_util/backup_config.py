import os

SNAPSHOT_TIMEOUT_SEC = int(os.environ.get("SNAPSHOT_TIMEOUT_SEC", "120"))
SNAPSHOT_POLL_INTERVAL_SEC = int(os.environ.get("SNAPSHOT_POLL_INTERVAL_SEC", "5"))
VOLUME_SNAPSHOT_CLASS = os.environ.get("VOLUME_SNAPSHOT_CLASS", "simplyblock-csi-snapshotclass")
