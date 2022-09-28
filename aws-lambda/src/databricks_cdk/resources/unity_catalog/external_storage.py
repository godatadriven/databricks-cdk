import json
import logging
from typing import Optional

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, delete_request, get_request, patch_request, post_request

logger = logging.getLogger(__name__)


class ExternalLocation(BaseModel):
    name: str
    comment: Optional[str]
    url: str
    credential_name: str
    read_only: bool = False


class ExternalLocationProperties(BaseModel):
    workspace_url: str
    external_location: ExternalLocation


class ExternalLocationResponse(CnfResponse):
    external_location_name: str


def get_external_location_url(workspace_url: str) -> str:
    """Getting url for job requests"""
    return f"{workspace_url}/api/2.1/unity-catalog/external-locations"


def get_external_location_by_name(name: str, base_url: str) -> Optional[dict]:
    return get_request(f"{base_url}/{name}")


def create_or_update_external_location(properties: ExternalLocationProperties) -> ExternalLocationResponse:
    """Create external_locations at databricks"""
    base_url = get_external_location_url(properties.workspace_url)
    current = get_external_location_by_name(properties.external_location.name, base_url=base_url)
    body = json.loads(properties.external_location.json())
    if current is None:
        post_request(base_url, body=body)
    else:
        patch_request(f"{base_url}/{properties.external_location.name}", body=body)
    return ExternalLocationResponse(
        external_location_name=properties.external_location.name,
        physical_resource_id=properties.external_location.name,
    )


def delete_external_location(properties: ExternalLocationProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes schema at databricks"""
    base_url = get_external_location_url(properties.workspace_url)
    current = get_external_location_by_name(properties.external_location.name, base_url=base_url)
    if current is not None:
        delete_request(f"{base_url}/{properties.external_location.name}")
    else:
        logger.warning("Already removed")
    return CnfResponse(physical_resource_id=physical_resource_id)
