import logging
from typing import Optional

from databricks.sdk.service.oauth2 import SecretInfo
from databricks.sdk import AccountClient
from databricks.sdk.errors import NotFound
from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, get_account_client

logger = logging.getLogger(__name__)


class ServicePrincipalSecretsCreationError(Exception):
    pass


class ServicePrincipalSecretsProperties(BaseModel):
    service_principal_id: int


def create_or_update_service_principal_secrets(
        properties: ServicePrincipalSecretsProperties,
        physical_resource_id: Optional[str] = None
    ) -> CnfResponse:
    """
    Create or update service principal secrets on databricks.
    If service principal secrets already exist, it will return the existing service principal secrets.
    If service principal secrets doesn't exist, it will create a new one.
    """
    account_client = get_account_client()
    
    if physical_resource_id:
        existing_service_principal_secrets = get_service_principal_secrets(
            service_principal_id=properties.service_principal_id,
            physical_resource_id=physical_resource_id,
            account_client=account_client
        )
        return CnfResponse(
            physical_resource_id=existing_service_principal_secrets.id
        )
    
    return create_service_principal_secrets(properties, account_client)


def get_service_principal_secrets(
        service_principal_id: int,
        physical_resource_id: str,
        account_client: AccountClient
    ) -> SecretInfo:
    """Get service principal secrets on databricks based on physical resource id and service principal id."""
    existing_service_principal_secrets = account_client.service_principal_secrets.list(
        service_principal_id=service_principal_id
    )

    for secret_info in existing_service_principal_secrets:
        if secret_info is not None and secret_info.id == physical_resource_id:
            return secret_info
    else:
        raise NotFound(f"Service principal secrets with id {physical_resource_id} not found")


def create_service_principal_secrets(properties: ServicePrincipalSecretsProperties, account_client: AccountClient) -> CnfResponse:
    """Create service principal secrets on databricks."""
    created_service_principal_secrets = account_client.service_principal_secrets.create(
        service_principal_id=properties.service_principal_id
    )

    if created_service_principal_secrets.id is None:
        raise ServicePrincipalSecretsCreationError("Failed to create service principal secrets")

    return CnfResponse(
        physical_resource_id=created_service_principal_secrets.id
    )


def delete_service_principal_secrets(properties: ServicePrincipalSecretsProperties, physical_resource_id: str) -> CnfResponse:
    """Delete service pricncipal secrets on databricks."""
    account_client = get_account_client()
    try:
        account_client.service_principal_secrets.delete(
            service_principal_id=properties.service_principal_id,
            secret_id=physical_resource_id
        )
    except NotFound:
        logger.warning("Service principal secrets with id %s not found", physical_resource_id)

    return CnfResponse(physical_resource_id=physical_resource_id)
