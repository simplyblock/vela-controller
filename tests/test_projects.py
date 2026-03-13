import pytest
from conftest import _id

_PROJECT_NAME = "test-project-lifecycle"
_PROJECT_UPDATED = "test-project-lifecycle-upd"
_state: dict = {}


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-projects")


def test_project_list_empty(client, org):
    r = client.get(f"organizations/{org}/projects/")
    assert r.status_code == 200
    assert r.json() == []


def test_project_create(client, org):
    r = client.post(
        f"organizations/{org}/projects/",
        json={
            "name": _PROJECT_NAME,
            "max_backups": 0,
            "project_limits": {},
            "per_branch_limits": {},
        },
    )
    assert r.status_code == 201
    assert "Location" in r.headers
    _state["project_id"] = _id(r.headers["Location"])


def test_project_get(client, org):
    r = client.get(f"organizations/{org}/projects/{_state['project_id']}/")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == str(_state["project_id"])
    assert data["name"] == _PROJECT_NAME
    assert data["organization_id"] == str(org)


def test_project_update(client, org):
    r = client.put(
        f"organizations/{org}/projects/{_state['project_id']}/",
        json={"name": _PROJECT_UPDATED},
    )
    assert r.status_code == 204


def test_project_get_updated(client, org):
    r = client.get(f"organizations/{org}/projects/{_state['project_id']}/")
    assert r.status_code == 200
    assert r.json()["name"] == _PROJECT_UPDATED


def test_project_list_contains(client, org):
    r = client.get(f"organizations/{org}/projects/")
    assert r.status_code == 200
    ids = [str(p["id"]) for p in r.json()]
    assert str(_state["project_id"]) in ids


def test_project_not_found(client, org):
    r = client.get(f"organizations/{org}/projects/00000000000000000000000000/")
    assert r.status_code == 404
