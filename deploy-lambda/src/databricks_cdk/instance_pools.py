import logging
from typing import List, Optional

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, get_request, post_request

logger = logging.getLogger(__name__)


class DockerBasicAuth(BaseModel):
    username: str
    password: str


class DockerImage(BaseModel):
    url: str
    basic_auth: Optional[DockerBasicAuth]


class InstancePoolAwsAttributes(BaseModel):
    spot_bid_price_percent: Optional[int] = None
    availability: str = "SPOT_WITH_FALLBACK"
    zone_id: str


class InstancePool(BaseModel):
    instance_pool_name: str
    min_idle_instances: Optional[int] = None
    max_capacity: Optional[int] = None
    aws_attributes: Optional[InstancePoolAwsAttributes] = None
    node_type_id: str
    custom_tags: Optional[dict] = None
    idle_instance_autotermination_minutes: Optional[int] = None
    enable_elastic_disk: bool = False
    disk_spec: Optional[dict] = None
    preloaded_spark_versions: List[str]
    preloaded_docker_images: List[DockerImage] = []


class InstancePoolProperties(BaseModel):
    action: str = "instance-pool"
    workspace_url: str
    instance_pool: InstancePool


class InstancePoolResponse(CnfResponse):
    instance_pool_id: str


class InstancePoolEdit(BaseModel):
    instance_pool_id: str
    instance_pool_name: str
    min_idle_instances: Optional[int]
    max_capacity: Optional[int]
    idle_instance_autotermination_minutes: Optional[int]


def get_instance_pools_url(workspace_url: str):
    """Getting url for instance_pools requests"""
    # api-endpoint
    return f"{workspace_url}/api/2.0/instance-pools"


def get_instance_pool_by_id(instance_pool_id: str, workspace_url: str) -> Optional[dict]:
    """Getting instance_pool by id"""
    body = {"instance_pool_id": instance_pool_id}

    return get_request(f"{get_instance_pools_url(workspace_url=workspace_url)}/get", body=body)


def create_or_update_instance_pool(properties: InstancePoolProperties, physical_resource_id: Optional[str]):
    """Create or update instance pool at Databricks"""
    url = get_instance_pools_url(properties.workspace_url)
    current: Optional[dict] = None
    instance_pool_properties = properties.instance_pool

    if physical_resource_id is not None:
        current = get_instance_pool_by_id(physical_resource_id, properties.workspace_url)

    if current is None:
        create_response = post_request(f"{url}/create", body=instance_pool_properties.dict())
        instance_pool_id = create_response.get("instance_pool_id")
        return InstancePoolResponse(instance_pool_id=instance_pool_id, physical_resource_id=instance_pool_id)
    else:
        instance_pool_id = current.get("instance_pool_id")
        instance_pool_edit = InstancePoolEdit(
            instance_pool_id=instance_pool_id,
            instance_pool_name=instance_pool_properties.instance_pool_name,
            min_idle_instances=instance_pool_properties.min_idle_instances,
            max_capacity=instance_pool_properties.max_capacity,
            idle_instance_autotermination_minutes=instance_pool_properties.idle_instance_autotermination_minutes,
        )

        post_request(f"{url}/edit", body=instance_pool_edit.dict())
        return InstancePoolResponse(instance_pool_id=instance_pool_id, physical_resource_id=instance_pool_id)


def delete_instance_pool(properties: InstancePoolProperties, physical_resource_id: str):
    """Delete instance pool at databricks"""
    current = get_instance_pool_by_id(physical_resource_id, properties.workspace_url)

    if current is not None:
        body = {
            "instance_pool_id": physical_resource_id,
        }
        post_request(f"{get_instance_pools_url(properties.workspace_url)}/delete", body=body)
    else:
        logger.warning("Already removed")
    return CnfResponse(physical_resource_id=physical_resource_id)
