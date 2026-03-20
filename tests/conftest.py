import os
import time
from collections.abc import Generator

import httpx
import pytest
from ulid import ULID

VELA_API_URL = os.getenv("VELA_API_URL", "http://localhost:8000/vela")
VELA_KEYCLOAK_URL = os.getenv("VELA_KEYCLOAK_URL", "http://localhost:8080")
VELA_TEST_EMAIL = os.getenv("VELA_TEST_EMAIL", "testuser@example.com")
VELA_TEST_PASSWORD = os.getenv("VELA_TEST_PASSWORD", "testpassword")
VELA_KEYCLOAK_CLIENT_ID = os.getenv("VELA_KEYCLOAK_CLIENT_ID", "controller")
VELA_KEYCLOAK_CLIENT_SECRET = os.getenv("VELA_KEYCLOAK_CLIENT_SECRET", "controller-secret")
BRANCH_TIMEOUT_SEC = int(os.getenv("BRANCH_TIMEOUT_SEC", "900"))

_POLL_INTERVAL = 10


class _KeycloakAuth(httpx.Auth):
    """httpx.Auth that automatically re-fetches the token before it expires."""

    def __init__(self) -> None:
        self._token: str = ""
        self._expires_at: float = 0.0

    def _refresh(self) -> None:
        url = f"{VELA_KEYCLOAK_URL}/auth/realms/vela/protocol/openid-connect/token"
        r = httpx.post(
            url,
            data={
                "grant_type": "password",
                "client_id": VELA_KEYCLOAK_CLIENT_ID,
                "client_secret": VELA_KEYCLOAK_CLIENT_SECRET,
                "username": VELA_TEST_EMAIL,
                "password": VELA_TEST_PASSWORD,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        self._token = data["access_token"]
        # Renew 60 s before the token actually expires
        self._expires_at = time.monotonic() + data.get("expires_in", 300) - 60

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        if time.monotonic() >= self._expires_at:
            self._refresh()
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


def wait_for_status(client: httpx.Client, url: str, expected: str, timeout: int = BRANCH_TIMEOUT_SEC) -> dict:
    """Poll GET url every 10s until response.json()["status"] == expected or timeout."""
    deadline = time.monotonic() + timeout
    while True:
        r = client.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        current = data.get("status")
        if current == expected:
            return data
        if current == "ERROR":
            raise RuntimeError(f"Resource entered ERROR state while waiting for status={expected!r}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for status={expected!r}; last status={current!r}")
        time.sleep(_POLL_INTERVAL)


def _id(location: str) -> ULID:
    """Extract ULID from a Location header value like /vela/organizations/{id}/"""
    return ULID.from_str(location.rstrip("/").rsplit("/", 1)[-1])


@pytest.fixture(scope="session")
def client():
    base = VELA_API_URL.rstrip("/") + "/"
    c = httpx.Client(base_url=base, auth=_KeycloakAuth(), timeout=30)
    yield c
    c.close()


@pytest.fixture(scope="session")
def make_org(client):
    created: list[ULID] = []

    def _factory(name: str) -> ULID:
        r = client.post("organizations/", json={"name": name, "max_backups": 0, "environments": ""})
        assert r.status_code == 201
        uid = _id(r.headers["Location"])
        created.append(uid)
        return uid

    yield _factory
    for uid in created:
        client.delete(f"organizations/{uid}/")


@pytest.fixture(scope="session")
def make_project(client):
    created: list[tuple[ULID, ULID]] = []

    def _factory(org_id: ULID, name: str) -> ULID:
        r = client.post(
            f"organizations/{org_id}/projects/",
            json={"name": name, "max_backups": 0, "project_limits": {}, "per_branch_limits": {}},
        )
        assert r.status_code == 201
        uid = _id(r.headers["Location"])
        created.append((org_id, uid))
        return uid

    yield _factory
    for org_id, uid in created:
        client.delete(f"organizations/{org_id}/projects/{uid}/")
