from unittest.mock import patch

from databricks_cdk.resources.sql_warehouses.sql_warehouses import (
    SQLWarehouse,
    SQLWarehouseProperties,
    create_or_update_warehouse,
    delete_warehouse,
)
from databricks_cdk.utils import CnfResponse


@patch("databricks_cdk.resources.sql_warehouses.sql_warehouses.post_request")
def test_create_instance_pool(patched_get_post_request):
    patched_get_post_request.return_value = {"warehouse_id": "some_id"}

    warehouse = SQLWarehouse(
        name="test",
        cluster_size="XXSMALL",
        max_num_clusters=1,
    )
    warehouse_properties = SQLWarehouseProperties(
        workspace_url="https://dbc-test.cloud.databricks.com",
        warehouse=warehouse,
    )

    # first without physical resource id then resource doesn't exist yet
    response = create_or_update_warehouse(warehouse_properties, None)
    assert response.warehouse_id == "some_id"
    assert response.physical_resource_id == "some_id"

    # make sure create endpoint is called
    assert patched_get_post_request.call_args.args[0] == "https://dbc-test.cloud.databricks.com/2.0/sql/warehouses/"


@patch("databricks_cdk.resources.sql_warehouses.sql_warehouses.get_warehouse_by_id")
@patch("databricks_cdk.resources.sql_warehouses.sql_warehouses.post_request")
def test_update_instance_pool(patched_get_post_request, patched_get_warehouse_by_id):
    patched_get_warehouse_by_id.return_value = {"warehouse_id": "some_id"}
    patched_get_post_request.return_value = {"warehouse_id": "some_id"}

    warehouse = SQLWarehouse(name="test", cluster_size="XXSMALL", max_num_clusters=1)

    warehouse_properties = SQLWarehouseProperties(
        workspace_url="https://dbc-test.cloud.databricks.com",
        warehouse=warehouse,
    )

    # first without physical resource id then resource doesn't exist yet
    response = create_or_update_warehouse(warehouse_properties, "some_id")
    assert response.warehouse_id == "some_id"
    assert response.physical_resource_id == "some_id"

    # # make sure edit endpoint is called
    assert (
        patched_get_post_request.call_args.args[0]
        == "https://dbc-test.cloud.databricks.com/2.0/sql/warehouses/some_id/edit"
    )


@patch("databricks_cdk.resources.sql_warehouses.sql_warehouses.get_warehouse_by_id")
@patch("databricks_cdk.resources.sql_warehouses.sql_warehouses.delete_request")
def test_delete_instance_pool(patched_get_delete_request, patched_get_warehouse_by_id):
    patched_get_warehouse_by_id.return_value = {"warehouse_id": "some_id"}
    patched_get_delete_request.return_value = {"warehouse_id": "some_id"}

    warehouse = SQLWarehouse(
        name="test",
        cluster_size="XXSMALL",
        max_num_clusters=1,
    )
    warehouse_properties = SQLWarehouseProperties(
        workspace_url="https://dbc-test.cloud.databricks.com",
        warehouse=warehouse,
    )

    response = delete_warehouse(properties=warehouse_properties, physical_resource_id="some_id")

    assert isinstance(response, CnfResponse)
    assert response.physical_resource_id == "some_id"
