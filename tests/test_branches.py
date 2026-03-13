import pytest
from conftest import BRANCH_TIMEOUT_SEC, _id, wait_for_status

pytestmark = pytest.mark.branch

_BRANCH_NAME = "test-branch"
_BRANCH_RENAMED = "test-branch-renamed"
_BRANCH_CREATE_PAYLOAD = {
    "name": _BRANCH_NAME,
    "deployment": {
        "database_password": "SecurePass1!",
        "database_size": 1000000000,
        "storage_size": 1000000000,
        "milli_vcpu": 500,
        "memory_bytes": 1073741824,
        "iops": 1000,
        "database_image_tag": "18.1-velaos",
        "enable_file_storage": True,
    },
}

_state: dict = {}


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-branches")


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-branches")


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
    # Teardown: delete branch (may already be deleted by test_branch_delete)
    client.delete(f"organizations/{org}/projects/{project}/branches/{bid}/", timeout=60)


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


def test_branch_password_reset(client, org, project, branch_id):
    r = client.post(
        f"organizations/{org}/projects/{project}/branches/{branch_id}/reset-password",
        json={"new_password": "NewPass1!"},
    )
    assert r.status_code == 204


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


def test_branch_delete(client, org, project, branch_id):
    r = client.delete(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 204
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code in (404, 410)
