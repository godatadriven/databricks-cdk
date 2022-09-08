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


class SQLWarehousePermissionsProperties(BaseModel):
    action: str = "warehouse-permissions"
    workspace_url: str
    endpoint_id: str
    access_control_list: List[Union[UserPermission, GroupPermission, ServicePrincipalPermission]]


def get_warehouse_permissions_url(workspace_url: str, endpoint_id: str):
    """Getting url for warehouse permissions requests"""
    return f"{workspace_url}/api/2.0/permissions/sql/endpoints/{endpoint_id}"


def create_or_update_warehouse_permissions(
    properties: SQLWarehousePermissionsProperties,
) -> CnfResponse:
    """Replace warrehouse permissions at databricks"""

    # Json data
    body = {
        "access_control_list": [a.dict() for a in properties.access_control_list],
    }
    put_request(
        f"{get_warehouse_permissions_url(properties.workspace_url, properties.cluster_id)}",
        body=body,
    )
    return CnfResponse(
        physical_resource_id=f"{properties.cluster_id}/permissions",
    )
