import logging
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, delete_request, get_request, post_request

logger = logging.getLogger(__name__)


class WarehouseTags(BaseModel):
    key: str
    value: str


class SQLWarehouse(BaseModel):
    name: str
    cluster_size: str
    min_num_clusters: Optional[int] = None
    max_num_clusters: int
    auto_stop_mins: Optional[int] = None
    tags: Optional[List[WarehouseTags]] = None
    spot_instance_policy: Optional[str] = None
    enable_photon: Optional[bool] = None
    enable_serverless_compute: Optional[bool] = None
    channel: Optional[str] = None


class SQLWarehouseEdit(BaseModel):
    id: Optional[str]
    name: Optional[str]
    cluster_size: Optional[str]
    min_num_clusters: Optional[int]
    max_num_clusters: Optional[int]
    auto_stop_mins: Optional[int]
    tags: Optional[List[WarehouseTags]]
    spot_instance_policy: Optional[str]
    enable_photon: Optional[bool]
    enable_serverless_compute: Optional[bool]
    channel: Optional[str]


class SQLWarehouseProperties(BaseModel):
    action: str = "warehouse"
    workspace_url: str
    warehouse: SQLWarehouse


class SQLWarehouseResponse(CnfResponse):
    warehouse_id: str


def get_warehouse_url(workspace_url: str):
    """Getting url for SQL Warehouse requests"""
    return f"{workspace_url}/api/2.0/sql/warehouses/"


def get_warehouse_by_id(warehouse_id: str, workspace_url: str) -> Optional[dict]:
    """Getting warehouse by id"""
    return get_request(f"{workspace_url}/2.0/sql/warehouses/{warehouse_id}")


def create_or_update_warehouse(properties: SQLWarehouseProperties, physical_resource_id: Optional[str]):
    """Create or update warehouse at Databricks"""
    url = get_warehouse_url(properties.workspace_url)
    current: Optional[dict] = None
    warehouse_properties = properties.warehouse

    if physical_resource_id is not None:
        current = get_warehouse_by_id(physical_resource_id, properties.workspace_url)

    if current is None:
        create_response = post_request(url=url, body=warehouse_properties.dict())
        warehouse_id = create_response.get("warehouse_id")
        post_request(url, body=warehouse_properties.dict())
        return SQLWarehouseResponse(warehouse_id=warehouse_id, physical_resource_id=warehouse_id)
    else:
        warehouse_id = current.get("warehouse_id")
        warehouse_edit = SQLWarehouseEdit(
            id=warehouse_id,
            name=warehouse_properties.name,
            cluster_size=warehouse_properties.cluster_size,
            min_num_clusters=warehouse_properties.min_num_clusters,
            max_num_clusters=warehouse_properties.max_num_clusters,
            auto_stop_mins=warehouse_properties.auto_stop_mins,
            tags=warehouse_properties.tags,
            spot_instance_policy=warehouse_properties.spot_instance_policy,
            enable_photon=warehouse_properties.enable_photon,
            enable_serverless_compute=warehouse_properties.enable_serverless_compute,
            channel=warehouse_properties.channel,
        )

        post_request(f"{url}{warehouse_id}/edit", body=warehouse_edit.dict())
        return SQLWarehouseResponse(warehouse_id=warehouse_id, physical_resource_id=warehouse_id)


def delete_warehouse(properties: SQLWarehouseProperties, physical_resource_id: str):
    """Delete warehouse at databricks"""
    current = get_warehouse_by_id(physical_resource_id, properties.workspace_url)

    if current is not None:
        body = {
            "warehouse_id": physical_resource_id,
        }
        delete_request(f"{get_warehouse_url(properties.workspace_url)}{physical_resource_id}")
    else:
        logger.warning("Already removed")
    return CnfResponse(physical_resource_id=physical_resource_id)
