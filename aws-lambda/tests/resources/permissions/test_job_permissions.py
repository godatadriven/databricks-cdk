from unittest.mock import patch

from databricks_cdk.resources.permissions.job_permissions import (
    JobPermissionsProperties,
    create_or_update_job_permissions,
    delete_job_permissions,
    get_job_permissions_url,
)
from databricks_cdk.resources.permissions.models import UserPermission
from databricks_cdk.utils import CnfResponse


def test_get_job_permissions_url():
    workspace_url = "https://test.cloud.databricks.com"
    job_id = "ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f"

    assert (
        get_job_permissions_url(workspace_url, job_id)
        == "https://test.cloud.databricks.com/api/2.0/permissions/jobs/ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f"
    )


@patch("databricks_cdk.resources.permissions.job_permissions.put_request")
def test_create_job_permissions(patched_get_put_request):
    job_permissions = JobPermissionsProperties(
        workspace_url="https://dbc-test.cloud.databricks.com",
        job_id="ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f",
        access_control_list=[UserPermission(permission_level="CAN_MANAGE", user_name="test")],
        owner_permission=UserPermission(permission_level="IS_OWNER", user_name="owner"),
    )

    response = create_or_update_job_permissions(job_permissions)

    assert response.physical_resource_id == "ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f/permissions"

    body_called = patched_get_put_request.call_args.kwargs["body"]
    assert len(body_called["access_control_list"]) == 2
    assert body_called["access_control_list"][0]["permission_level"] == "CAN_MANAGE"
    assert body_called["access_control_list"][0]["user_name"] == "test"
    assert body_called["access_control_list"][1]["permission_level"] == "IS_OWNER"
    assert body_called["access_control_list"][1]["user_name"] == "owner"

    assert (
        patched_get_put_request.call_args.args[0]
        == "https://dbc-test.cloud.databricks.com/api/2.0/permissions/jobs/ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f"
    )


@patch("databricks_cdk.resources.permissions.job_permissions.put_request")
def test_delete_job_permissions(patched_get_put_request):
    job_permissions = JobPermissionsProperties(
        workspace_url="https://dbc-test.cloud.databricks.com",
        job_id="ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f",
        access_control_list=[UserPermission(permission_level="CAN_MANAGE", user_name="test")],
        owner_permission=UserPermission(permission_level="IS_OWNER", user_name="owner"),
    )
    response = delete_job_permissions(job_permissions, "ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f")
    assert response == CnfResponse(physical_resource_id="ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f")

    body_called = patched_get_put_request.call_args.kwargs["body"]
    assert len(body_called["access_control_list"]) == 1
    assert body_called["access_control_list"][0]["permission_level"] == "IS_OWNER"
    assert body_called["access_control_list"][0]["user_name"] == "owner"
