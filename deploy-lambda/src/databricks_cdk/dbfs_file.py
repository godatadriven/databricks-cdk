import logging

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, post_request

logger = logging.getLogger(__name__)


class DbfsFileProperties(BaseModel):
    action: str = "dbfs-file"
    workspace_url: str
    path: str
    base64_bytes: str


class DbfsFileResponse(CnfResponse):
    dbfs_path: str
    file_path: str


def get_dbfs_file_url(workspace_url: str):
    """Getting url for dbfs_file requests"""
    return f"{workspace_url}/api/2.0/dbfs"


def create_or_update_dbfs_file(properties: DbfsFileProperties) -> DbfsFileResponse:
    """Create dbfs_file at databricks"""
    url = get_dbfs_file_url(properties.workspace_url)
    create_body = {"path": properties.path, "overwrite": True}
    create_response = post_request(f"{url}/create", body=create_body)
    handle = create_response.get("handle")

    add_body = {
        "data": properties.base64_bytes,
        "handle": handle,
    }
    post_request(f"{get_dbfs_file_url(properties.workspace_url)}/add-block", body=add_body)
    post_request(f"{get_dbfs_file_url(properties.workspace_url)}/close", body={"handle": handle})

    return DbfsFileResponse(
        physical_resource_id=properties.path, dbfs_path=f"dbfs:{properties.path}", file_path=f"/dbfs{properties.path}"
    )


def delete_dbfs_file(properties: DbfsFileProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes dbfs_file at databricks"""
    url = get_dbfs_file_url(properties.workspace_url)
    post_request(f"{url}/delete", body={"path": properties.path})

    return CnfResponse(physical_resource_id=physical_resource_id)
