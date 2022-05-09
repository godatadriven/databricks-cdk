import logging
from typing import Dict, List, Optional

import requests
from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, get_auth, get_request, post_request

logger = logging.getLogger(__name__)


class AutoScale(BaseModel):
    min_workers: int
    max_workers: int


class AwsAttributes(BaseModel):
    first_on_demand: int = 1
    availability: str = "SPOT_WITH_FALLBACK"
    zone_id: str
    instance_profile_arn: Optional[str] = None
    spot_bid_price_percent: Optional[int] = None
    ebs_volume_type: Optional[str] = None
    ebs_volume_count: Optional[int] = None
    ebs_volume_size: Optional[int] = None
    ebs_volume_iops: Optional[int] = None
    ebs_volume_throughput: Optional[int] = None


class DockerBasicAuth(BaseModel):
    username: str
    password: str


class DockerImage(BaseModel):
    url: str
    basic_auth: Optional[DockerBasicAuth]


class Cluster(BaseModel):
    num_workers: Optional[int] = None
    autoscale: Optional[AutoScale] = None
    cluster_name: str
    spark_version: str
    spark_conf: Dict[str, str] = {}
    aws_attributes: AwsAttributes
    node_type_id: Optional[str] = None
    driver_node_type_id: Optional[str] = None
    ssh_public_keys: List[str] = []
    custom_tags: Optional[dict] = None
    cluster_log_conf: Optional[dict] = None
    init_scripts: List[dict] = []
    docker_image: Optional[DockerImage] = None
    spark_env_vars: Dict[str, str] = {}
    autotermination_minutes: Optional[int] = None
    enable_elastic_disk: Optional[bool] = None
    driver_instance_pool_id: Optional[str] = None
    instance_pool_id: Optional[str] = None
    idempotency_token: Optional[str] = None
    apply_policy_default_values: Optional[bool] = None
    enable_local_disk_encryption: Optional[bool] = None


class ClusterProperties(BaseModel):
    action: str = "cluster"
    workspace_url: str
    cluster: Cluster


def get_cluster_url(workspace_url: str):
    """Getting url for cluster requests"""
    return f"{workspace_url}/api/2.0/clusters"


def get_cluster_by_name(cluster_name: str, workspace_url: str):
    resp = get_request(f"{get_cluster_url(workspace_url)}/list")
    clusters = resp.get("clusters", [])
    for c in clusters:
        if c.get("cluster_name") == cluster_name:
            return c
    return None


def get_cluster_by_id(cluster_id: str, workspace_url: str) -> Optional[dict]:
    """Getting cluster based on name"""
    body = {"cluster_id": cluster_id}
    auth = get_auth()
    resp = requests.get(f"{get_cluster_url(workspace_url)}/get", json=body, headers={}, auth=auth)
    if resp.status_code == 400 and "does not exist" in resp.text:
        return None
    resp.raise_for_status()
    return resp.json()


def create_or_update_cluster(properties: ClusterProperties, physical_resource_id: Optional[str]) -> CnfResponse:
    """Create cluster at databricks"""
    current = None
    if physical_resource_id is not None:
        current = get_cluster_by_id(physical_resource_id, properties.workspace_url)
    if current is None:

        # Json data
        body = properties.cluster.dict()
        response = post_request(f"{get_cluster_url(properties.workspace_url)}/create", body=body)
        return CnfResponse(
            physical_resource_id=response["cluster_id"],
        )
    else:
        cluster_id = current["cluster_id"]
        body = properties.cluster.dict()
        body["cluster_id"] = cluster_id
        post_request(f"{get_cluster_url(properties.workspace_url)}/edit", body=body)
        return CnfResponse(
            physical_resource_id=cluster_id,
        )


def delete_cluster(properties: ClusterProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes cluster at databricks"""
    current = get_cluster_by_id(physical_resource_id, properties.workspace_url)
    if current is not None:
        body = {
            "cluster_id": current.get("cluster_id"),
        }
        post_request(f"{get_cluster_url(properties.workspace_url)}/permanent-delete", body=body)
    else:
        logger.warning("Already removed")
    return CnfResponse(physical_resource_id=physical_resource_id)
