import pytest
from conftest import BRANCH_TIMEOUT_SEC, _id, wait_for_status

pytestmark = pytest.mark.backup

_BRANCH_NAME = "test-branch-backup"
_BRANCH_PASSWORD = "SecurePass1!"
_BRANCH_CREATE_PAYLOAD = {
    "name": _BRANCH_NAME,
    "deployment": {
        "database_password": _BRANCH_PASSWORD,
        "database_size": 5000000000,
        "storage_size": 5000000000,
        "milli_vcpu": 500,
        "memory_bytes": 1073741824,
        "iops": 1000,
        "database_image_tag": "18.1-velaos",
        "enable_file_storage": False,
    },
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-backup", max_backups=10)


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-backup", max_backups=10)


@pytest.fixture(scope="module")
def branch_id(client, org, project):
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/",
        json=_BRANCH_CREATE_PAYLOAD,
        timeout=60,
    )
    assert r.status_code == 201
    bid = _id(r.headers["Location"])
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{bid}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )

    yield bid

    r = client.delete(f"organizations/{org}/projects/{project}/branches/{bid}/")
    assert r.status_code == 204


@pytest.fixture(scope="module")
def backup_id(client, branch_id):
    """Trigger a manual backup and return the backup_id."""
    r = client.post(f"backup/branches/{branch_id}/", timeout=120)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "manual backup created"
    assert "backup_id" in data
    return data["backup_id"]


# ---------------------------------------------------------------------------
# Branch Clone
# ---------------------------------------------------------------------------


def test_branch_clone(client, org, project, branch_id):
    """Clone a branch and verify the clone reaches ACTIVE_HEALTHY."""
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/",
        json={
            "name": "test-branch-clone",
            "source": {
                "branch_id": str(branch_id),
                "data_copy": True,
            },
        },
        timeout=60,
    )
    assert r.status_code == 201
    clone_id = _id(r.headers["Location"])

    try:
        wait_for_status(
            client,
            f"organizations/{org}/projects/{project}/branches/{clone_id}/",
            "ACTIVE_HEALTHY",
            BRANCH_TIMEOUT_SEC,
        )
    finally:
        r = client.delete(f"organizations/{org}/projects/{project}/branches/{clone_id}/")
        assert r.status_code == 204


# ---------------------------------------------------------------------------
# Manual Backup
# ---------------------------------------------------------------------------


def test_manual_backup(backup_id):
    """Trigger an on-demand backup for the branch and verify a backup_id is returned."""
    assert backup_id, "backup_id fixture must return a valid id"


# ---------------------------------------------------------------------------
# Restore a new branch from backup
# ---------------------------------------------------------------------------


def test_restore_branch_from_backup(client, org, project, backup_id):
    """Create a new branch from the manual backup and verify it reaches ACTIVE_HEALTHY."""
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/",
        json={
            "name": "test-branch-restored",
            "restore": {"backup_id": backup_id},
        },
        timeout=60,
    )
    assert r.status_code == 201
    restored_id = _id(r.headers["Location"])

    try:
        wait_for_status(
            client,
            f"organizations/{org}/projects/{project}/branches/{restored_id}/",
            "ACTIVE_HEALTHY",
            BRANCH_TIMEOUT_SEC,
        )
    finally:
        r = client.delete(f"organizations/{org}/projects/{project}/branches/{restored_id}/")
        assert r.status_code == 204
