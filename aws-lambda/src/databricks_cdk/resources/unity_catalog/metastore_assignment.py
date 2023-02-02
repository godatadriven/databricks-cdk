import logging

from pydantic import BaseModel

from databricks_cdk.resources.unity_catalog.metastore import get_metastore_by_name, get_metastore_url
from databricks_cdk.utils import CnfResponse, delete_request, put_request

logger = logging.getLogger(__name__)


class AssignmentProperties(BaseModel):
    workspace_url: str
    workspace_id: str
    metastore_name: str
    default_catalog_name: str


class AssignmentResponse(CnfResponse):
    workspace_id: str


def get_assignment_url(workspace_url: str, workspace_id: str):
    """Getting url for job requests"""
    return f"{workspace_url}/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore"


def create_or_update_assignment(properties: AssignmentProperties) -> AssignmentResponse:
    """Create assignment at databricks"""
    base_url = get_assignment_url(properties.workspace_url, properties.workspace_id)
    metastore_result = get_metastore_by_name(
        properties.metastore_name, base_url=get_metastore_url(properties.workspace_url)
    )
    if metastore_result is None:
        raise RuntimeError(f"No metastore found with name {properties.metastore_name}")
    body = {
        "metastore_id": metastore_result.get("metastore_id"),
        "default_catalog_name": properties.default_catalog_name,
    }
    put_request(base_url, body=body)
    return AssignmentResponse(
        workspace_id=properties.workspace_id,
        physical_resource_id=properties.workspace_id,
    )


def delete_assignment(properties: AssignmentProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes assignment at databricks"""
    base_url = get_assignment_url(properties.workspace_url, properties.workspace_id)
    delete_request(base_url, body={"metastore_id": properties.metastore_id})
    return CnfResponse(physical_resource_id=physical_resource_id)
