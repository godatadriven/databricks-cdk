from unittest.mock import patch

from databricks_cdk.resources.permissions.cluster_policy_permissions import (
    ClusterPolicyPermissionsProperties,
    create_or_update_cluster_policy_permissions,
)
from databricks_cdk.resources.permissions.models import (
    GroupPermission,
    ServicePrincipalPermission,
    User,
    UserPermission,
)


@patch("databricks_cdk.resources.permissions.cluster_policy_permissions.put_request")
def test_create_cluster_policy_permissions(patched_get_put_request):
    cluster_policy_permissions = ClusterPolicyPermissionsProperties(
        workspace_url="https://dbc-test.cloud.databricks.com",
        cluster_policy_id="ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f",
        access_control_list=[
            UserPermission(permission_level="CAN_MANAGE", user_name="test_user"),
            GroupPermission(permission_level="CAN_MANAGE", group_name="test_group"),
        ],
        owner=User(user_name="owner"),
    )

    response = create_or_update_cluster_policy_permissions(cluster_policy_permissions)

    assert response.physical_resource_id == "ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f"

    body_called = patched_get_put_request.call_args.kwargs["body"]
    assert len(body_called["access_control_list"]) == 2
    assert body_called["access_control_list"][0]["permission_level"] == "CAN_MANAGE"
    assert body_called["access_control_list"][0]["user_name"] == "test_user"
    assert body_called["access_control_list"][1]["permission_level"] == "CAN_MANAGE"
    assert body_called["access_control_list"][1]["group_name"] == "test_group"

    assert (
        patched_get_put_request.call_args.args[0]
        == "https://dbc-test.cloud.databricks.com/api/2.0/permissions/cluster-policies/ae5850e0-8046-44e6-aa1e-6ec3e84f2f8f"
    )
