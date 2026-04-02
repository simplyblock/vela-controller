import pytest
from conftest import BRANCH_TIMEOUT_SEC, wait_for_status

pytestmark = pytest.mark.branch


@pytest.fixture(scope="module")
def branch_id(make_branch, org, project):
    return make_branch(org, project, "test-branch-start-stop")


def test_branch_stop(client, org, project, branch_id):
    r = client.post(f"organizations/{org}/projects/{project}/branches/{branch_id}/stop")
    assert r.status_code == 204
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "STOPPED",
        BRANCH_TIMEOUT_SEC,
    )


def test_branch_start(client, org, project, branch_id):
    r = client.post(f"organizations/{org}/projects/{project}/branches/{branch_id}/start")
    assert r.status_code == 204
    wait_for_status(
        client,
        f"organizations/{org}/projects/{project}/branches/{branch_id}/",
        "ACTIVE_HEALTHY",
        BRANCH_TIMEOUT_SEC,
    )
