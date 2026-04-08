import pytest
from conftest import BRANCH_TIMEOUT_SEC, wait_for_status


@pytest.fixture(scope="module")
def org(make_org):
    return make_org("test-org-start-stop")


@pytest.fixture(scope="module")
def project(make_project, org):
    return make_project(org, "test-project-start-stop")


@pytest.fixture(scope="module")
def branch_id(make_branch, org, project):
    return make_branch(org, project, "test-branch-start-stop")


def test_branch_stop(client, org, project, branch_id):
    r = client.post(f"organizations/{org}/projects/{project}/branches/{branch_id}/stop")
    assert r.status_code == 202
    assert "Location" in r.headers
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        ["STOPPING", "STOPPED"],
        BRANCH_TIMEOUT_SEC,
    )


def test_branch_start(client, org, project, branch_id):
    r = client.post(f"organizations/{org}/projects/{project}/branches/{branch_id}/start")
    assert r.status_code == 202
    assert "Location" in r.headers
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        ["STARTING", "ACTIVE_HEALTHY"],
        BRANCH_TIMEOUT_SEC,
    )
