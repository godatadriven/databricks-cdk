import logging
from typing import Optional

import cfnresponse
from pydantic import BaseModel

from databricks_cdk.cluster import ClusterProperties, create_or_update_cluster, delete_cluster
from databricks_cdk.cluster_permissions import (
    ClusterPermissionsProperties,
    create_or_update_cluster_permissions,
    delete_cluster_permissions,
)
from databricks_cdk.credentials import CredentialsProperties, create_or_update_credentials, delete_credentials
from databricks_cdk.dbfs_file import DbfsFileProperties, create_or_update_dbfs_file, delete_dbfs_file
from databricks_cdk.group import GroupProperties, create_or_update_group, delete_group
from databricks_cdk.instance_pools import InstancePoolProperties, create_or_update_instance_pool, delete_instance_pool
from databricks_cdk.instance_profile import (
    InstanceProfileProperties,
    create_or_update_instance_profile,
    delete_instance_profile,
)
from databricks_cdk.job import JobProperties, create_or_update_job, delete_job
from databricks_cdk.networks import NetworksProperties, create_or_update_networks, delete_networks
from databricks_cdk.secret import SecretProperties, create_or_update_secret, delete_secret
from databricks_cdk.secret_scope import SecretScopeProperties, create_or_update_secret_scope, delete_secret_scope
from databricks_cdk.storage_config import (
    StorageConfigProperties,
    create_or_update_storage_configuration,
    delete_storage_configuration,
)
from databricks_cdk.user import UserProperties, create_or_update_user, delete_user
from databricks_cdk.utils import CnfResponse
from databricks_cdk.workspace import WorkspaceProperties, create_or_update_workspaces, delete_workspaces

logger = logging.getLogger(__name__)


class DatabricksEvent(BaseModel):
    RequestType: str
    ResourceProperties: dict
    PhysicalResourceId: Optional[str] = None

    def action(self):
        return self.ResourceProperties.get("action")


def create_or_update_resource(event: DatabricksEvent) -> CnfResponse:
    """Creates or update a given resource"""
    action = event.action()
    if action == "credentials":
        return create_or_update_credentials(CredentialsProperties(**event.ResourceProperties))
    elif action == "storage-configurations":
        return create_or_update_storage_configuration(StorageConfigProperties(**event.ResourceProperties))
    elif action == "networks":
        return create_or_update_networks(NetworksProperties(**event.ResourceProperties))
    elif action == "workspaces":
        return create_or_update_workspaces(WorkspaceProperties(**event.ResourceProperties))
    elif action == "instance-profile":
        return create_or_update_instance_profile(InstanceProfileProperties(**event.ResourceProperties))
    elif action == "cluster":
        return create_or_update_cluster(ClusterProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "user":
        return create_or_update_user(UserProperties(**event.ResourceProperties))
    elif action == "cluster-permissions":
        return create_or_update_cluster_permissions(ClusterPermissionsProperties(**event.ResourceProperties))
    elif action == "group":
        return create_or_update_group(GroupProperties(**event.ResourceProperties))
    elif action == "dbfs-file":
        return create_or_update_dbfs_file(DbfsFileProperties(**event.ResourceProperties))
    elif action == "secret-scope":
        return create_or_update_secret_scope(SecretScopeProperties(**event.ResourceProperties))
    elif action == "secret":
        return create_or_update_secret(SecretProperties(**event.ResourceProperties))
    elif action == "job":
        return create_or_update_job(JobProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "instance-pool":
        return create_or_update_instance_pool(
            InstancePoolProperties(**event.ResourceProperties), event.PhysicalResourceId
        )
    else:
        raise RuntimeError(f"Unknown action: {action}")


def delete_resource(event: DatabricksEvent) -> CnfResponse:
    """Delete a given resource"""
    action = event.action()
    if action == "credentials":
        return delete_credentials(CredentialsProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "storage-configurations":
        return delete_storage_configuration(
            StorageConfigProperties(**event.ResourceProperties), event.PhysicalResourceId
        )
    elif action == "networks":
        return delete_networks(NetworksProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "workspaces":
        return delete_workspaces(WorkspaceProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "instance-profile":
        return delete_instance_profile(InstanceProfileProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "cluster":
        return delete_cluster(ClusterProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "user":
        return delete_user(UserProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "cluster-permissions":
        return delete_cluster_permissions(event.PhysicalResourceId)
    elif action == "group":
        return delete_group(GroupProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "dbfs-file":
        return delete_dbfs_file(DbfsFileProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "secret-scope":
        return delete_secret_scope(SecretScopeProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "secret":
        return delete_secret(SecretProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "job":
        return delete_job(JobProperties(**event.ResourceProperties), event.PhysicalResourceId)
    elif action == "instance-pool":
        return delete_instance_pool(InstancePoolProperties(**event.ResourceProperties), event.PhysicalResourceId)
    else:
        raise RuntimeError(f"Unknown action: {action}")


def process_event(event: DatabricksEvent) -> CnfResponse:
    """Process a databricks deploy event"""
    if event.RequestType == "Create" or event.RequestType == "Update":
        return create_or_update_resource(event)
    elif event.RequestType == "Delete":
        return delete_resource(event)
    else:
        logger.error(f"unknown request_type: {event.RequestType}")


def handler(event, context):
    """Entrypoint for lambda"""
    logger.info(event)
    try:
        parsed_event = DatabricksEvent(**event)
        response_data = process_event(parsed_event)
        cfnresponse.send(
            event,
            context,
            cfnresponse.SUCCESS,
            response_data.dict(),
            physicalResourceId=response_data.physical_resource_id,
        )
    except Exception as e:
        logger.exception(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, None)
