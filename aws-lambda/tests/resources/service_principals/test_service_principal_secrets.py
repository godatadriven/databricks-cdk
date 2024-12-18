from unittest.mock import patch

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sdk.service.oauth2 import CreateServicePrincipalSecretResponse, SecretInfo

from databricks_cdk.resources.service_principals.service_principal_secrets import (
    ServicePrincipalSecretsCreationError,
    ServicePrincipalSecretsProperties,
    create_or_update_service_principal_secrets,
    create_service_principal_secrets,
    delete_service_principal_secrets,
    get_service_principal_secrets,
)
from databricks_cdk.utils import CnfResponse


@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_account_client")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.create_service_principal_secrets")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_service_principal_secrets")
def test_create_or_update_service_principal_secrets_create(
    patched_get_service_principal_secrets,
    patched_create_service_principal_secrets,
    patched_get_account_client,
    account_client,
):
    patched_get_account_client.return_value = account_client
    mock_properties = ServicePrincipalSecretsProperties(service_principal_id=1)

    create_or_update_service_principal_secrets(properties=mock_properties)

    patched_create_service_principal_secrets.assert_called_once_with(
        mock_properties,
        account_client,
    )
    patched_get_service_principal_secrets.assert_not_called()


@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_account_client")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.create_service_principal_secrets")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_service_principal_secrets")
def test_create_or_update_service_principal_secrets_update(
    patched_get_service_principal_secrets,
    patched_create_service_principal_secrets,
    patched_get_account_client,
    account_client,
):
    patched_get_account_client.return_value = account_client
    mock_physical_resource_id = "some_id"
    existing_service_principal_secrets = SecretInfo(id=mock_physical_resource_id)
    patched_get_service_principal_secrets.return_value = existing_service_principal_secrets
    mock_properties = ServicePrincipalSecretsProperties(service_principal_id=1)

    response = create_or_update_service_principal_secrets(
        properties=mock_properties, physical_resource_id=mock_physical_resource_id
    )

    assert response == CnfResponse(physical_resource_id=mock_physical_resource_id)
    patched_get_service_principal_secrets.assert_called_once_with(
        service_principal_id=mock_properties.service_principal_id,
        physical_resource_id=mock_physical_resource_id,
        account_client=account_client,
    )
    patched_create_service_principal_secrets.assert_not_called()


def test_get_service_principal_secrets(account_client):
    service_principal_id = 1
    service_principal_secrets_id = "some_id"
    service_principal_info = SecretInfo(id=service_principal_secrets_id)
    account_client.service_principal_secrets.list.return_value = [service_principal_info]

    response = get_service_principal_secrets(service_principal_id, service_principal_secrets_id, account_client)
    assert response == service_principal_info
    account_client.service_principal_secrets.list.assert_called_once_with(service_principal_id=service_principal_id)


def test_get_service_principal_secrets_error(account_client):
    existing_service_principal_secrets = SecretInfo(id="some_different_id")
    account_client.service_principal_secrets.list.return_value = [existing_service_principal_secrets]

    with pytest.raises(NotFound):
        get_service_principal_secrets(1, "some_id", account_client)


def test_get_service_principal_secrets_error_no_secrets(account_client):
    account_client.service_principal_secrets.list.return_value = []

    with pytest.raises(NotFound):
        get_service_principal_secrets(1, "some_id", account_client)


@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_service_principal")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.add_to_secrets_manager")
def test_create_service_principal_secrets(
    patched_add_to_secrets_manager,
    patched_get_service_principal,
    account_client,
):
    patched_get_service_principal.return_value = ServicePrincipal(
        application_id="some_client_id",
        display_name="mock_name",
        id=1,
    )
    mock_properties = ServicePrincipalSecretsProperties(service_principal_id=1)
    account_client.service_principal_secrets.create.return_value = CreateServicePrincipalSecretResponse(
        id="some_id",
        secret="some_secret_id",
    )
    response = create_service_principal_secrets(mock_properties, account_client)

    assert response == CnfResponse(physical_resource_id="some_id")
    account_client.service_principal_secrets.create.assert_called_once_with(
        service_principal_id=1,
    )
    patched_add_to_secrets_manager.assert_called_once_with(
        secret_name="mock_name/1",
        client_id="some_client_id",
        client_secret="some_secret_id",
    )


def test_create_service_principal_secrets_error(account_client):
    mock_properties = ServicePrincipalSecretsProperties(service_principal_id=1)
    account_client.service_principal_secrets.create.return_value = CreateServicePrincipalSecretResponse(id=None)

    with pytest.raises(ServicePrincipalSecretsCreationError):
        create_service_principal_secrets(mock_properties, account_client)


@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_account_client")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.delete_from_secrets_manager")
@patch("databricks_cdk.resources.service_principals.service_principal_secrets.get_service_principal")
def test_delete_service_principal(
    patched_get_service_principal,
    patched_delete_from_secrets_manager,
    patched_get_account_client,
    account_client,
):
    patched_get_service_principal.return_value = ServicePrincipal(
        application_id="some_id",
        display_name="mock_name",
        id=1,
    )
    patched_get_account_client.return_value = account_client
    mock_properties = ServicePrincipalSecretsProperties(service_principal_id=1)
    response = delete_service_principal_secrets(mock_properties, "some_id")

    assert response == CnfResponse(physical_resource_id="some_id")
    account_client.service_principal_secrets.delete.assert_called_once_with(
        service_principal_id=1,
        secret_id="some_id",
    )
    patched_delete_from_secrets_manager.assert_called_once_with("mock_name/1")
