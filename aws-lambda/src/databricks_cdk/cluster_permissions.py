import logging
from typing import List, Union

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, put_request

logger = logging.getLogger(__name__)


class Permission(BaseModel):
    permission_level: str


class UserPermission(Permission):
    user_name: str


class GroupPermission(Permission):
    group_name: str


class ServicePrincipalPermission(Permission):
    service_principal_name: str


class ClusterPermissionsProperties(BaseModel):
    action: str = "cluster-permissions"
    workspace_url: str
    cluster_id: str
    access_control_list: List[Union[UserPermission, GroupPermission, ServicePrincipalPermission]]


def get_cluster_permissions_url(workspace_url: str, cluster_id: str):
    """Getting url for cluster_permissions requests"""
    return f"{workspace_url}/api/2.0/permissions/clusters/{cluster_id}"


def create_or_update_cluster_permissions(properties: ClusterPermissionsProperties) -> CnfResponse:
    """Create get_instance_profile_by_arn at databricks"""

    # Json data
    body = {
        "access_control_list": [a.dict() for a in properties.access_control_list],
    }
    put_request(f"{get_cluster_permissions_url(properties.workspace_url, properties.cluster_id)}", body=body)
    return CnfResponse(
        physical_resource_id=f"{properties.cluster_id}/permissions",
    )


def delete_cluster_permissions(physical_resource_id: str) -> CnfResponse:
    """Deletes get_instance_profile_by_arn at databricks"""
    # no need to remove
    return CnfResponse(physical_resource_id=physical_resource_id)
