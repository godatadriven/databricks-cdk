from unittest.mock import patch

from databricks_cdk.resources.instance_pools.instance_pools import (
    InstancePool,
    InstancePoolProperties,
    create_or_update_instance_pool,
    delete_instance_pool,
    get_instance_pools_url,
)
from databricks_cdk.utils import CnfResponse


def test_get_instance_pools_url():
    test_workspace_url = "https://dbc-test.cloud.databricks.com"
    assert get_instance_pools_url(test_workspace_url) == "https://dbc-test.cloud.databricks.com/api/2.0/instance-pools"


@patch("databricks_cdk.resources.instance_pools.instance_pools.post_request")
def test_create_instance_pool(patched_get_post_request):
    patched_get_post_request.return_value = {"instance_pool_id": "some_id"}

    instance_pool = InstancePool(
        instance_pool_name="test", node_type_id="test", preloaded_spark_versions=["some_version"]
    )
    instance_pool_properties = InstancePoolProperties(
        workspace_url="https://dbc-test.cloud.databricks.com", instance_pool=instance_pool
    )

    # first without physical resource id then resource doesn't exist yet
    response = create_or_update_instance_pool(instance_pool_properties, None)

    assert response.instance_pool_id == "some_id"
    assert response.physical_resource_id == "some_id"

    # make sure create endpoint is called
    assert (
        patched_get_post_request.call_args.args[0]
        == "https://dbc-test.cloud.databricks.com/api/2.0/instance-pools/create"
    )


@patch("databricks_cdk.resources.instance_pools.instance_pools.get_instance_pool_by_id")
@patch("databricks_cdk.resources.instance_pools.instance_pools.post_request")
def test_update_instance_pool(patched_get_post_request, patched_get_instance_pool_by_id):
    patched_get_instance_pool_by_id.return_value = {"instance_pool_id": "some_id"}
    patched_get_post_request.return_value = {"instance_pool_id": "some_id"}

    instance_pool = InstancePool(
        instance_pool_name="test", node_type_id="test", preloaded_spark_versions=["some_version"]
    )
    instance_pool_properties = InstancePoolProperties(
        workspace_url="https://dbc-test.cloud.databricks.com", instance_pool=instance_pool
    )

    # first without physical resource id then resource doesn't exist yet
    response = create_or_update_instance_pool(instance_pool_properties, "some_id")
    assert response.instance_pool_id == "some_id"
    assert response.physical_resource_id == "some_id"

    # make sure edit endpoint is called
    assert (
        patched_get_post_request.call_args.args[0]
        == "https://dbc-test.cloud.databricks.com/api/2.0/instance-pools/edit"
    )


@patch("databricks_cdk.resources.instance_pools.instance_pools.get_instance_pool_by_id")
@patch("databricks_cdk.resources.instance_pools.instance_pools.post_request")
def test_delete_instance_pool(patched_get_post_request, patched_get_instance_pool_by_id):
    patched_get_instance_pool_by_id.return_value = {"instance_pool_id": "some_id"}
    patched_get_post_request.return_value = {"instance_pool_id": "some_id"}

    instance_pool = InstancePool(
        instance_pool_name="test", node_type_id="test", preloaded_spark_versions=["some_version"]
    )
    instance_pool_properties = InstancePoolProperties(
        workspace_url="https://dbc-test.cloud.databricks.com", instance_pool=instance_pool
    )

    response = delete_instance_pool(properties=instance_pool_properties, physical_resource_id="some_id")

    assert isinstance(response, CnfResponse)
    assert response.physical_resource_id == "some_id"
