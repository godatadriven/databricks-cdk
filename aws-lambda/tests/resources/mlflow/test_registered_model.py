from unittest.mock import patch

import pytest
from databricks.sdk.service.ml import CreateModelResponse, GetModelResponse, Model, ModelDatabricks, ModelTag

from databricks_cdk.resources.mlflow.registered_model import (
    RegisteredModelCreateResponse,
    RegisteredModelProperties,
    _update_registered_model_tags,
    create_or_update_registered_model,
    delete_registered_model,
)


def test__update_registered_model_tags_add(workspace_client):
    props = RegisteredModelProperties(
        name="test-model",
        tags=[ModelTag(key="test", value="test-value")],
        workspace_url="https://test.cloud.databricks.com",
    )
    _update_registered_model_tags(
        workspace_client=workspace_client,
        properties=props,
        current_tags=[],
    )

    workspace_client.model_registry.set_model_tag.assert_called_once_with("test-model", "test", "test-value")


def test__update_registered_model_tags_update(workspace_client):
    props = RegisteredModelProperties(
        name="test-model",
        tags=[ModelTag(key="test", value="new-test-value")],
        workspace_url="https://test.cloud.databricks.com",
    )
    _update_registered_model_tags(
        workspace_client=workspace_client,
        properties=props,
        current_tags=[
            ModelTag(key="to-delete", value="test-delete"),
            ModelTag(key="test", value="test-value"),
        ],
    )

    # Make sure tag test-model is updated
    workspace_client.model_registry.set_model_tag.assert_called_once_with("test-model", "test", "new-test-value")

    # Make sure removed to-delete tag is deleted
    workspace_client.model_registry.delete_model_tag.assert_called_once_with("test-model", "to-delete")


@patch("databricks_cdk.resources.mlflow.registered_model.get_workspace_client")
def test_create_or_update_registered_model_new(patched_get_workspace_client, workspace_client):
    patched_get_workspace_client.return_value = workspace_client

    workspace_client.model_registry.create_model.return_value = CreateModelResponse(
        registered_model=Model(name="new-model-name")
    )
    props = RegisteredModelProperties(
        name="new-model-name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    # completely new experiment
    response = create_or_update_registered_model(props, physical_resource_id=None)
    assert response == RegisteredModelCreateResponse(physical_resource_id="new-model-name")


@patch("databricks_cdk.resources.mlflow.registered_model.get_workspace_client")
def test_create_or_update_registered_model_existing(patched_get_workspace_client, workspace_client):
    patched_get_workspace_client.return_value = workspace_client

    workspace_client.model_registry.get_model.return_value = GetModelResponse(
        registered_model_databricks=ModelDatabricks(name="model-name", description="same description")
    )

    props = RegisteredModelProperties(
        name="new-model-name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    response = create_or_update_registered_model(props, physical_resource_id="new-model-name")

    assert response == RegisteredModelCreateResponse(physical_resource_id="new-model-name")
    workspace_client.model_registry.update_model.assert_called_once_with(
        name="new-model-name", description="same description"
    )


@patch("databricks_cdk.resources.mlflow.registered_model.get_workspace_client")
def test_create_or_update_registered_model_invalid(patched_get_workspace_client, workspace_client):
    props = RegisteredModelProperties(
        name="new-model-name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    patched_get_workspace_client.return_value = workspace_client
    workspace_client.model_registry.get_model.return_value = None

    # this is invalid and should raise error
    # patched__get_existing_registered_model.return_value = None
    with pytest.raises(ValueError):
        create_or_update_registered_model(props, physical_resource_id="model-name")


@patch("databricks_cdk.resources.mlflow.registered_model.get_workspace_client")
def test_delete_experiment(patched_get_workspace_client, workspace_client):
    patched_get_workspace_client.return_value = workspace_client
    props = RegisteredModelProperties(
        name="name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    delete_registered_model(props, "name")
    workspace_client.model_registry.delete_model.assert_called_once_with(name="name")
