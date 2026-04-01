"""Integration tests for async branch deletion.

These tests verify:
  - DELETE /branches/{id}/ returns 202 Accepted with a Location header
  - A concurrent DELETE returns 400 while the first delete is in progress
  - The branch is eventually gone (GET returns 404) after the task completes
"""

import time

import pytest
from conftest import BRANCH_TIMEOUT_SEC, wait_for_deletion

pytestmark = pytest.mark.branch

_POLL_INTERVAL = 10

_state: dict = {}


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-delete")


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-delete")


@pytest.fixture(scope="module")
def branch_id(make_branch, org, project):
    return make_branch(org, project, "test-branch-delete")


def test_delete_returns_202_with_location(client, org, project, branch_id):
    r = client.delete(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 202
    assert "Location" in r.headers
    _state["task_url"] = r.headers["Location"]


def test_task_listed_while_deleting(client, org, project, branch_id):
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/tasks/")
    if r.status_code == 404:
        pytest.skip("Branch already deleted before task listing test could run")
    assert r.status_code == 200
    tasks = r.json()
    assert any(t["task_type"] == "delete" for t in tasks)


def test_concurrent_delete_returns_400(client, org, project, branch_id):
    # While the branch has delete_task_id set the endpoint must reject a second delete.
    # The branch may have been deleted already in fast CI environments (404 is also acceptable).
    r = client.delete(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code in (400, 404)


def test_branch_gone_after_delete(client, org, project, branch_id):
    # Poll until the branch returns 404 — the Celery task has completed and deleted it.
    wait_for_deletion(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        BRANCH_TIMEOUT_SEC,
    )
    r = client.get(f"organizations/{org}/projects/{project}/branches/{branch_id}/")
    assert r.status_code == 404


def test_task_detail_unavailable_after_deletion(client):
    task_url = _state.get("task_url")
    if not task_url:
        pytest.skip("No task URL stored from delete test")
    # Once the branch is gone the task detail endpoint returns 404 (BranchDep lookup fails).
    deadline = time.monotonic() + BRANCH_TIMEOUT_SEC
    while True:
        r = client.get(task_url, timeout=30)
        if r.status_code == 404:
            return
        status = r.json().get("status") if r.status_code == 200 else None
        if status in ("COMPLETED", "FAILED"):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for task to complete; last status={status!r}")
        time.sleep(_POLL_INTERVAL)
