import time

import psycopg
import pytest

pytestmark = pytest.mark.backup

_BRANCH_PASSWORD = "SecurePass1!"


def _execute_sql(db_info: dict, password: str, *statements: str) -> list[tuple]:
    """Execute one or more SQL statements with retries for DNS propagation delay.

    Returns the result of the *last* statement that produces rows.
    """
    host = db_info["host"]
    port = db_info["port"] or 5432
    deadline = time.monotonic() + 120  # Max wait
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with (
                psycopg.connect(
                    host=host,
                    port=port,
                    dbname=db_info["name"],
                    user=db_info["username"],
                    password=password,
                    connect_timeout=10,
                    autocommit=True,
                ) as conn,
                conn.cursor() as cur,
            ):
                result: list[tuple] = []
                for sql in statements:
                    cur.execute(sql)
                    if cur.description:
                        result = cur.fetchall()
                return result
        except psycopg.OperationalError as exc:
            last_exc = exc
        time.sleep(15)
    raise AssertionError(f"Could not connect to postgres at {host}:{port}: {last_exc}") from last_exc


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
def populated_branch_id(client, org, project, branch_id):
    """Wait for the branch, connect to its DB, and populate data."""
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    db_info = r.json()["database"]

    _execute_sql(
        db_info,
        _BRANCH_PASSWORD,
        "CREATE TABLE test_data_integrity (id SERIAL PRIMARY KEY, value TEXT)",
        "INSERT INTO test_data_integrity (value) VALUES ('original_data')",
    )

    return branch_id


@pytest.fixture(scope="module")
def backup_id(client, populated_branch_id):
    """Trigger a manual backup and return the backup_id."""
    r = client.post(f"backup/branches/{populated_branch_id}/", timeout=120)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "manual backup created"
    assert "backup_id" in data
    return data["backup_id"]


def test_branch_clone(client, org, project, populated_branch_id, make_branch):
    """Clone a branch and verify the clone reaches ACTIVE_HEALTHY."""
    clone_id = make_branch(
        org,
        project,
        "test-branch-clone",
        source={"branch_id": str(populated_branch_id), "data_copy": True},
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{clone_id}/")
    assert r.status_code == 200
    branch_data = r.json()
    assert branch_data["status"] == "ACTIVE_HEALTHY"

    rows = _execute_sql(branch_data["database"], _BRANCH_PASSWORD, "SELECT value FROM test_data_integrity")
    assert rows == [("original_data",)], f"Expected [('original_data',)], got {rows}"


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
    branch_data = r.json()
    assert branch_data["status"] == "ACTIVE_HEALTHY"

    rows = _execute_sql(branch_data["database"], _BRANCH_PASSWORD, "SELECT value FROM test_data_integrity")
    assert rows == [("original_data",)], f"Expected [('original_data',)], got {rows}"
