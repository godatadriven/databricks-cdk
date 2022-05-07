from databricks_cdk.instance_pools import (
    delete_instance_pool,
    get_instance_pool_by_id,
    get_instance_pools_url,
    InstancePool,
    InstancePoolProperties,
    create_or_update_instance_pool,
    delete_instance_pool,
)

workspace_url = "https://dbc-9670e4d5-ed08.cloud.databricks.com"

# test = get_instance_pool_by_id(
#     workspace_url=workspace_url, instance_pool_id="0428-070006-josh8-pool-kl8vt9fx"
# )
# '0428-085041-rank9-pool-3kjrkn9p'

new_instance_pool = InstancePool(
    instance_pool_name="test_instance_pool_new",
    min_idle_instances=None,
    max_capacity=None,
    node_type_id="i3.xlarge",
    custom_tags={"test": "test"},
    idle_instance_autotermination_minutes=None,
    preloaded_spark_versions=["9.1.x-scala2.12"],
)

properties = InstancePoolProperties(
    action="instance-pool", workspace_url=workspace_url, instance_pool=new_instance_pool
)

test = delete_instance_pool(
    properties=properties, physical_resource_id="0428-093532-fines10-pool-yyqpzy19"
)
test