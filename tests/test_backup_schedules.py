import time
from datetime import datetime

import pytest

pytestmark = pytest.mark.backup

_state: dict = {}

_BACKUP_POLL_SEC = 15
_BACKUP_TIMEOUT_SEC = 300


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-schedules", max_backups=10)


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-schedules", max_backups=10)


@pytest.fixture(scope="module")
def branch_id(make_branch, org, project):
    return make_branch(org, project, "test-branch-schedules")


def test_scheduled_backup_created_on_time(client, branch_id):
    """Create a 1-min schedule, wait for the backup monitor to fire,
    and verify the backup arrives on time with a valid PVC snapshot."""
    r = client.post(
        f"backup/branches/{branch_id}/schedule",
        json={"rows": [{"row_index": 0, "interval": 1, "unit": "min", "retention": 3}]},
    )
    assert r.status_code == 200
    _state["sched_id"] = r.json()["schedule_id"]

    # Wait for the backup monitor to initialise NextBackup records
    deadline = time.monotonic() + _BACKUP_TIMEOUT_SEC
    info = None
    while time.monotonic() < deadline:
        r = client.get(f"backup/branches/{branch_id}/info")
        if r.status_code == 200:
            info = r.json()
            break
        time.sleep(_BACKUP_POLL_SEC)
    assert info is not None, "Backup info endpoint never became available"
    expected_next = datetime.fromisoformat(info["next_backup"])

    # Poll until a scheduled backup (row_index == 0) appears
    backup = None
    while time.monotonic() < deadline:
        r = client.get(f"backup/branches/{branch_id}/")
        assert r.status_code == 200
        scheduled = [b for b in r.json() if b["row_index"] == 0]
        if scheduled:
            backup = scheduled[0]
            break
        time.sleep(_BACKUP_POLL_SEC)

    assert backup is not None, "Scheduled backup was not created within timeout"
    assert backup["size_bytes"] > 0, "PVC snapshot was not created (size_bytes is 0)"

    created_at = datetime.fromisoformat(backup["created_at"])
    drift = abs((created_at - expected_next).total_seconds())
    assert drift < 180, f"Backup created {drift:.0f}s from expected time ({expected_next})"

    _state["scheduled_backup_id"] = backup["id"]


def test_scheduled_backup_cleanup(client):
    r = client.delete(f"backup/{_state['scheduled_backup_id']}", timeout=120)
    assert r.status_code == 200

    r = client.delete(f"backup/schedule/{_state['sched_id']}/")
    assert r.status_code == 200
    assert r.json()["status"] == "success"
