from conftest import _id

_ORG_NAME = "test-org-lifecycle"
_ORG_UPDATED = "test-org-lifecycle-upd"
_state: dict = {}


def test_org_list_empty(client):
    r = client.get("organizations/")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_org_create(client):
    r = client.post(
        "organizations/",
        json={"name": _ORG_NAME, "max_backups": 0, "environments": ""},
    )
    assert r.status_code == 201
    assert "Location" in r.headers
    _state["org_id"] = _id(r.headers["Location"])


def test_org_get(client):
    r = client.get(f"organizations/{_state['org_id']}/")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == str(_state["org_id"])
    assert data["name"] == _ORG_NAME


def test_org_update(client):
    r = client.put(
        f"organizations/{_state['org_id']}/",
        json={"name": _ORG_UPDATED},
    )
    assert r.status_code == 204


def test_org_get_updated(client):
    r = client.get(f"organizations/{_state['org_id']}/")
    assert r.status_code == 200
    assert r.json()["name"] == _ORG_UPDATED


def test_org_list_contains(client):
    r = client.get("organizations/")
    assert r.status_code == 200
    ids = [str(o["id"]) for o in r.json()]
    assert str(_state["org_id"]) in ids


def test_org_not_found(client):
    r = client.get("organizations/00000000000000000000000000/")
    assert r.status_code == 404


def test_org_delete(client):
    r = client.delete(f"organizations/{_state['org_id']}/")
    assert r.status_code == 204
    r = client.get(f"organizations/{_state['org_id']}/")
    assert r.status_code == 404
