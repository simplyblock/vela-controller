import pytest

_GiB = 1024**3
_GB = 10**9

_ORG_LIMITS = {
    "total": {"milli_vcpu": 2000, "ram": 2 * _GiB, "iops": 200, "database_size": 20 * _GB, "storage_size": 20 * _GB},
    "per_branch": {
        "milli_vcpu": 1000,
        "ram": 1 * _GiB,
        "iops": 100,
        "database_size": 10 * _GB,
        "storage_size": 10 * _GB,
    },
}
_PROJECT_LIMITS = {
    "total": {"milli_vcpu": 1000, "ram": 1 * _GiB, "iops": 100, "database_size": 10 * _GB, "storage_size": 10 * _GB},
    "per_branch": {"milli_vcpu": 500, "ram": _GiB // 2, "iops": 50, "database_size": 5 * _GB, "storage_size": 5 * _GB},
}

_FIELDS = {"milli_vcpu", "ram", "iops", "database_size", "storage_size"}

# Stored during the test run to reuse in later assertions.
_org_available: dict = {}


# ---------------------------------------------------------------------------
# Module-scoped fixtures — isolated from the session fixtures used by
# test_branches.py so that the limits we set here don't leak into other tests.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-resources")


# ---------------------------------------------------------------------------
# Org default limits (must run before test_org_set_limits changes them)
# ---------------------------------------------------------------------------


def test_org_has_default_limits_on_creation(client, org) -> None:
    """New org should have limits populated from OrganizationLimitDefault defaults."""
    r = client.get(f"organizations/{org}/resources/limits/")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) == {"total", "per_branch"}
    for section in ("total", "per_branch"):
        assert set(data[section].keys()) == _FIELDS
        for v in data[section].values():
            assert v is not None and isinstance(v, int) and v > 0


def test_org_default_limits_are_available(client, org) -> None:
    """Available resources for a new org should equal its default limits (no branches yet)."""
    limits_r = client.get(f"organizations/{org}/resources/limits/")
    avail_r = client.get(f"organizations/{org}/resources/available/")
    assert limits_r.status_code == 200
    assert avail_r.status_code == 200
    for f in _FIELDS:
        assert avail_r.json()[f] == limits_r.json()["total"][f]


# ---------------------------------------------------------------------------
# Org (override limits)
# ---------------------------------------------------------------------------


def test_org_set_limits(client, org) -> None:
    r = client.put(f"organizations/{org}/resources/limits/", json=_ORG_LIMITS)
    assert r.status_code == 204


def test_org_limits_configured(client, org) -> None:
    r = client.get(f"organizations/{org}/resources/limits/")
    assert r.status_code == 200
    assert r.json() == _ORG_LIMITS


def test_org_allocations_zero(client, org) -> None:
    r = client.get(f"organizations/{org}/resources/allocations/")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) == _FIELDS
    for v in data.values():
        assert v == 0


def test_org_available(client, org) -> None:
    r = client.get(f"organizations/{org}/resources/available/")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) == _FIELDS
    for f in _FIELDS:
        assert data[f] == _ORG_LIMITS["total"][f]
    _org_available.update(data)


# ---------------------------------------------------------------------------
# Project WITHOUT limits (expect HTTP 500 until bug is fixed)
# ---------------------------------------------------------------------------


def test_project_no_limits_available(client, org, project) -> None:
    r = client.get(f"organizations/{org}/projects/{project}/resources/available/")
    # Known bug: AssertionError in Resources.__sub__ → HTTP 500.
    # This test is expected to FAIL until the bug is fixed.
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) == _FIELDS
    for f in _FIELDS:
        assert data[f] == _org_available[f]


# ---------------------------------------------------------------------------
# Project allocations (no branches)
# ---------------------------------------------------------------------------


def test_project_allocations_zero(client, org, project) -> None:
    r = client.get(f"organizations/{org}/projects/{project}/resources/allocations/")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) == _FIELDS
    for v in data.values():
        assert v == 0


# ---------------------------------------------------------------------------
# Project WITH limits
# ---------------------------------------------------------------------------


def test_project_set_limits(client, org, project) -> None:
    r = client.put(
        f"organizations/{org}/projects/{project}/resources/limits/",
        json=_PROJECT_LIMITS,
    )
    assert r.status_code == 204


def test_project_limits_configured(client, org, project) -> None:
    r = client.get(f"organizations/{org}/projects/{project}/resources/limits/")
    assert r.status_code == 200
    assert r.json() == _PROJECT_LIMITS


def test_project_available(client, org, project) -> None:
    r = client.get(f"organizations/{org}/projects/{project}/resources/available/")
    assert r.status_code == 200
    data = r.json()
    assert set(data.keys()) == _FIELDS
    for f in _FIELDS:
        assert data[f] == _PROJECT_LIMITS["total"][f]
