from typing import List, Union

from pydantic import BaseModel

from databricks_cdk.resources.permissions.models import GroupPermission, ServicePrincipalPermission, UserPermission
from databricks_cdk.utils import CnfResponse, put_request


class JobPermissionsProperties(BaseModel):
    action: str = "job-permissions"  #
    workspace_url: str
    job_id: str
    access_control_list: List[Union[UserPermission, GroupPermission, ServicePrincipalPermission]] = []
    owner_name: str


def get_job_permissions_url(workspace_url: str, job_id: str):
    """Getting url for job permissions requests"""
    return f"{workspace_url}/api/2.0/permissions/jobs/{job_id}"


def create_or_update_job_permissions(properties: JobPermissionsProperties) -> CnfResponse:
    """Create job permissions on job at databricks"""

    # Json data
    access_control_list = [a.dict() for a in properties.access_control_list]
    # Always add owner
    access_control_list.append(UserPermission(permission_level="IS_OWNER", user_name=properties.owner_name).dict())

    body = {
        "access_control_list": access_control_list,
    }
    put_request(
        f"{get_job_permissions_url(properties.workspace_url, properties.job_id)}",
        body=body,
    )
    return CnfResponse(
        physical_resource_id=f"{properties.job_id}/permissions",
    )


def delete_job_permissions(properties: JobPermissionsProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes all job permissions on job at databricks"""
    # remove everything but the owner
    body = {
        "access_control_list": [UserPermission(permission_level="IS_OWNER", user_name=properties.owner_name).dict()],
    }

    put_request(
        f"{get_job_permissions_url(properties.workspace_url, properties.job_id)}",
        body=body,
    )

    return CnfResponse(physical_resource_id=physical_resource_id)
