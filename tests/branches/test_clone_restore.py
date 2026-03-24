import pytest

pytestmark = pytest.mark.backup

_BRANCH_PASSWORD = "SecurePass1!"


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-backup", max_backups=10)


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-backup", max_backups=10)


@pytest.fixture(scope="module")
def branch_id(make_branch, org, project):
    return make_branch(
        org,
        project,
        "test-branch-backup",
        deployment={
            "database_password": _BRANCH_PASSWORD,
            "database_size": 5_000_000_000,
            "storage_size": 5_000_000_000,
            "milli_vcpu": 500,
            "memory_bytes": 1_073_741_824,
            "iops": 1000,
            "database_image_tag": "18.1-velaos",
            "enable_file_storage": False,
        },
    )


@pytest.fixture(scope="module")
def backup_id(client, branch_id):
    """Trigger a manual backup and return the backup_id."""
    r = client.post(f"backup/branches/{branch_id}/", timeout=120)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "manual backup created"
    assert "backup_id" in data
    return data["backup_id"]


def test_branch_clone(client, org, project, branch_id, make_branch):
    """Clone a branch and verify the clone reaches ACTIVE_HEALTHY."""
    clone_id = make_branch(
        org,
        project,
        "test-branch-clone",
        source={"branch_id": str(branch_id), "data_copy": True},
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{clone_id}/")
    assert r.status_code == 200
    assert r.json()["status"] == "ACTIVE_HEALTHY"


def test_manual_backup(backup_id):
    """Trigger an on-demand backup for the branch and verify a backup_id is returned."""
    assert backup_id, "backup_id fixture must return a valid id"


def test_restore_branch_from_backup(client, org, project, backup_id, make_branch):
    """Create a new branch from the manual backup and verify it reaches ACTIVE_HEALTHY."""
    restored_id = make_branch(
        org,
        project,
        "test-branch-restored",
        restore={"backup_id": backup_id},
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{restored_id}/")
    assert r.status_code == 200
    assert r.json()["status"] == "ACTIVE_HEALTHY"
