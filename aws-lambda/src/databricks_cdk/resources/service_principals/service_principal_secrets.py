import json
import logging
from typing import Optional

import boto3
from databricks.sdk import AccountClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.oauth2 import SecretInfo
from pydantic import BaseModel

from databricks_cdk.resources.service_principals.service_principal import get_service_principal
from databricks_cdk.utils import CnfResponse, get_account_client

logger = logging.getLogger(__name__)


class ServicePrincipalSecretsCreationError(Exception):
    pass


class ServicePrincipalSecretsProperties(BaseModel):
    service_principal_id: int


def create_or_update_service_principal_secrets(
    properties: ServicePrincipalSecretsProperties, physical_resource_id: Optional[str] = None
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
            account_client=account_client,
        )
        return CnfResponse(physical_resource_id=existing_service_principal_secrets.id)

    return create_service_principal_secrets(properties, account_client)


def get_service_principal_secrets(
    service_principal_id: int, physical_resource_id: str, account_client: AccountClient
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


def create_service_principal_secrets(
    properties: ServicePrincipalSecretsProperties, account_client: AccountClient
) -> CnfResponse:
    """
    Create service principal secrets on databricks.
    It will create a new service principal secrets and store it in secrets manager.
    """
    service_principal = get_service_principal(properties.service_principal_id, account_client)
    created_service_principal_secrets = account_client.service_principal_secrets.create(
        service_principal_id=properties.service_principal_id
    )

    if created_service_principal_secrets.id is None:
        raise ServicePrincipalSecretsCreationError("Failed to create service principal secrets")

    secret_name = f"{service_principal.display_name}/{service_principal.id}"
    add_to_secrets_manager(
        secret_name=secret_name,
        client_id=service_principal.application_id,
        client_secret=created_service_principal_secrets.secret,
    )
    return CnfResponse(physical_resource_id=created_service_principal_secrets.id)


def delete_service_principal_secrets(
    properties: ServicePrincipalSecretsProperties, physical_resource_id: str
) -> CnfResponse:
    """Delete service pricncipal secrets on databricks. It will delete the service principal secrets from databricks and secrets manager."""
    account_client = get_account_client()

    try:
        account_client.service_principal_secrets.delete(
            service_principal_id=properties.service_principal_id, secret_id=physical_resource_id
        )
    except NotFound:
        logger.warning("Service principal secrets with id %s not found", physical_resource_id)

    service_principal = get_service_principal(properties.service_principal_id, account_client)
    secret_name = f"{service_principal.display_name}/{service_principal.id}"
    delete_from_secrets_manager(secret_name)
    return CnfResponse(physical_resource_id=physical_resource_id)


def add_to_secrets_manager(secret_name: str, client_id: str, client_secret: str) -> None:
    """Adds credentials to secrets manager at /databricks/service_principal/secrets/{{secret_name}}"""
    client = boto3.client("secretsmanager")
    secret_full_name = f"/databricks/service_principal/secrets/{secret_name}"
    secret_string = {"client_id": client_id, "client_secret": client_secret}
    client.create_secret(Name=secret_full_name, SecretString=json.dumps(secret_string))["ARN"]


def delete_from_secrets_manager(secret_name: str) -> None:
    """Removes credentials from secrets manager at /databricks/service_principal/secrets/{{secret_name}}"""
    client = boto3.client("secretsmanager")
    secret_full_name = f"/databricks/service_principal/secrets/{secret_name}"
    try:
        client.delete_secret(SecretId=secret_full_name, ForceDeleteWithoutRecovery=True)
    except client.exceptions.ResourceNotFoundException:
        logger.warning("Secrets are not found in secrets manager")
