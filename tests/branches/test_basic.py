import time

import psycopg
import pytest
from conftest import BRANCH_TIMEOUT_SEC, wait_for_status

pytestmark = pytest.mark.branch

_BRANCH_PASSWORD = "SecurePass1!"
_BRANCH_NAME = "test-branch"
_BRANCH_RENAMED = "test-branch-renamed"

_DB_CONNECT_TIMEOUT = 10
_DB_CONNECT_MAX_WAIT = 120
_DB_CONNECT_RETRY_DELAY = 15

_state: dict = {}


def _check_postgres_connection(db_info: dict, password: str) -> None:
    """Verify a Postgres connection can be established with the given credentials.

    Retries for up to _DB_CONNECT_MAX_WAIT seconds to handle DNS propagation
    delay after the branch reaches ACTIVE_HEALTHY. Falls back to port 5432
    when the API returns 0 (no NodePort configured; database exposed via LB).
    """
    host = db_info["host"]
    port = db_info["port"] or 5432
    deadline = time.monotonic() + _DB_CONNECT_MAX_WAIT
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host=host,
                port=port,
                dbname=db_info["name"],
                user=db_info["username"],
                password=password,
                connect_timeout=_DB_CONNECT_TIMEOUT,
            ):
                return
        except psycopg.OperationalError as exc:
            last_exc = exc
        time.sleep(_DB_CONNECT_RETRY_DELAY)
    raise AssertionError(f"Could not connect to postgres at {host}:{port}: {last_exc}") from last_exc


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-branches")


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-branches")


@pytest.fixture(scope="module")
def branch_id(client, make_branch, org, project):
    bid = make_branch(
        org,
        project,
        _BRANCH_NAME,
        deployment={
            "database_password": _BRANCH_PASSWORD,
            "database_size": 1_000_000_000,
            "storage_size": 1_000_000_000,
            "milli_vcpu": 500,
            "memory_bytes": 1_073_741_824,
            "iops": 1000,
            "database_image_tag": "18.1-velaos",
            "enable_file_storage": True,
        },
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{bid}/")
    r.raise_for_status()
    _check_postgres_connection(r.json()["database"], _BRANCH_PASSWORD)
    return bid


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_branch_list_empty(client, org, project):
    r = client.get(f"organizations/{org}/projects/{project}/branches/")
    assert r.status_code == 200
    assert r.json() == []


def test_branch_create(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["status"] == "ACTIVE_HEALTHY"


def test_branch_get(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == str(branch_id)
    assert data["name"] == _BRANCH_NAME
    assert "status" in data
    assert "database" in data


def test_branch_update(client, org, project, branch_id):
    r = client.put(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        json={"name": _BRANCH_RENAMED},
    )
    assert r.status_code == 204


def test_branch_get_renamed(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["name"] == _BRANCH_RENAMED


def test_branch_list_contains(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/")
    assert r.status_code == 200
    ids = [str(b["id"]) for b in r.json()]
    assert str(branch_id) in ids


def test_branch_status_endpoint(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/status")
    assert r.status_code == 200
    data = r.json()
    assert "resize_status" in data


def test_branch_resize(client, org, project, branch_id):
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/resize",
        json={"iops": 2000},
    )
    assert r.status_code == 202
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["max_resources"]["iops"] == 2000


def test_branch_resize_cpu(client, org, project, branch_id):
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/resize",
        json={"milli_vcpu": 1000},
    )
    assert r.status_code == 202
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["max_resources"]["milli_vcpu"] == 1000


def test_branch_resize_memory(client, org, project, branch_id):
    # 2 GiB expressed in bytes (must be a multiple of 256 MiB)
    two_gib = 2 * 1024 * 1024 * 1024
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/resize",
        json={"memory_bytes": two_gib},
    )
    assert r.status_code == 202
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["max_resources"]["ram_bytes"] == two_gib


def test_branch_resize_database_size(client, org, project, branch_id):
    # 6 GB expressed in bytes (must be a multiple of 1 GB)
    six_gb = 6 * 1_000_000_000
    # due to issues related to round up
    seven_gb = 7 * 1_000_000_000
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/resize",
        json={"database_size": six_gb},
    )
    assert r.status_code == 202
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["max_resources"]["nvme_bytes"] == seven_gb


def test_branch_resize_storage_size(client, org, project, branch_id):
    # 2 GB expressed in bytes (must be a multiple of 1 GB)
    two_gb = 2 * 1_000_000_000
    # due to GiB rounding by the storage backend: 2 GB -> 2 GiB -> rounds up to 3 GB
    three_gb = 3 * 1_000_000_000
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/resize",
        json={"storage_size": two_gb},
    )
    assert r.status_code == 202
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    assert r.json()["max_resources"]["storage_bytes"] == three_gb


def test_branch_password_reset(client, org, project, branch_id):
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/reset-password",
        json={"new_password": "NewPass1!"},
    )
    assert r.status_code == 204
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 200
    _check_postgres_connection(r.json()["database"], "NewPass1!")


def test_branch_apikey_create(client, org, project, branch_id):
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/apikeys/",
        json={"name": "test-key", "role": "anon", "expiry": "30d"},
    )
    assert r.status_code == 201
    _state["api_key_id"] = r.json()["id"]


def test_branch_apikey_list(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/apikeys/")
    assert r.status_code == 200
    ids = [k["id"] for k in r.json()]
    assert _state["api_key_id"] in ids
