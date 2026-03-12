import os

import httpx


def test_health_ok(client):
    r = client.get("health")
    assert r.status_code == 200


def test_unauthenticated_rejected():
    base = os.getenv("VELA_API_URL", "http://localhost:8000/vela").rstrip("/") + "/"
    r = httpx.get(f"{base}organizations/", timeout=30)
    assert r.status_code == 401
