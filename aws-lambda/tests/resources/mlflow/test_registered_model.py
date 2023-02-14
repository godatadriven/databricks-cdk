from unittest.mock import patch

import pytest

from databricks_cdk.resources.mlflow.registered_model import (
    RegisteredModel,
    RegisteredModelCreateResponse,
    RegisteredModelProperties,
    RegisteredModelTag,
    _create_registered_model,
    _get_registered_model,
    _update_registered_model_description,
    _update_registered_model_name,
    _update_registered_model_tags,
    create_or_update_registered_model,
    delete_registered_model,
    get_registered_model_url,
)


def test_get_registered_model_url():
    workspace_url = "https://test.cloud.databricks.com"
    assert (
        get_registered_model_url(workspace_url) == "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models"
    )


@patch("databricks_cdk.resources.mlflow.registered_model.post_request")
def test__create_registered_model(patched_post_request):
    props = RegisteredModelProperties(
        name="test",
        workspace_url="https://test.cloud.databricks.com",
        description="some description",
        tags=[RegisteredModelTag(key="test", value="test")],
    )
    patched_post_request.return_value = {"registered_model": {"name": "test"}}
    name = _create_registered_model("https://test.cloud.databricks.com/api/2.0/mlflow/registered-models", props)

    assert name == "test"
    assert patched_post_request.call_count == 1

    assert patched_post_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/create",
        {
            "name": "test",
            "description": "some description",
            "tags": [{"key": "test", "value": "test"}],
        },
    )


@patch("databricks_cdk.resources.mlflow.registered_model.get_request")
def test__get_registered_model(patched__get_request):
    patched__get_request.return_value = {
        "registered_model": {
            "name": "same_name",
            "creation_timestamp": 1,
            "last_updated_timestamp": 1,
            "description": "same description",
        }
    }

    registered_model = _get_registered_model(
        registered_model_url="https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        name="same_name",
    )

    assert isinstance(registered_model, RegisteredModel)

    patched__get_request.return_value = None
    assert not _get_registered_model(
        registered_model_url="https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        name="same_name",
    )


@patch("databricks_cdk.resources.mlflow.registered_model.post_request")
def test__update_registered_model_name(patched_post_request):
    _update_registered_model_name(
        registered_model_url="https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        current_name="name",
        new_name="new_name",
    )

    assert patched_post_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/rename",
        {"name": "name", "new_name": "new_name"},
    )


@patch("databricks_cdk.resources.mlflow.registered_model.patch_request")
def test__update_registered_model_description(patched_patch_request):
    _update_registered_model_description(
        registered_model_url="https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        registered_model_name="test",
        description="new_description",
    )

    assert (
        patched_patch_request.call_args.args[0]
        == "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/update"
    )

    assert patched_patch_request.call_args.kwargs == {"body": {"description": "new_description", "name": "test"}}


@patch("databricks_cdk.resources.mlflow.registered_model.post_request")
def test__update_registered_model_tags_add(patched_post_request):
    props = RegisteredModelProperties(
        name="test-model",
        tags=[RegisteredModelTag(key="test", value="test-value")],
        workspace_url="https://test.cloud.databricks.com",
    )
    _update_registered_model_tags(
        registered_model_url="https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        properties=props,
        current_tags=[],
    )

    assert (
        patched_post_request.call_args.args[0]
        == "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/set-tag"
    )
    assert patched_post_request.call_args.args[1] == {
        "name": "test-model",
        "key": "test",
        "value": "test-value",
    }


@patch("databricks_cdk.resources.mlflow.registered_model.delete_request")
@patch("databricks_cdk.resources.mlflow.registered_model.post_request")
def test__update_registered_model_tags_update(patched_post_request, patched_delete_request):
    props = RegisteredModelProperties(
        name="test-model",
        tags=[RegisteredModelTag(key="test", value="new-test-value")],
        workspace_url="https://test.cloud.databricks.com",
    )
    _update_registered_model_tags(
        registered_model_url="https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        properties=props,
        current_tags=[
            RegisteredModelTag(key="to-delete", value="test-delete"),
            RegisteredModelTag(key="test", value="test-value"),
        ],
    )

    # Make sure tag test-model is updated
    assert (
        patched_post_request.call_args.args[0]
        == "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/set-tag"
    )
    assert patched_post_request.call_args.args[1] == {
        "name": "test-model",
        "key": "test",
        "value": "new-test-value",
    }

    # Make sure removed to-delete tag is deleted
    assert (
        patched_delete_request.call_args.args[0]
        == "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/delete-tag"
    )
    assert patched_delete_request.call_args.kwargs == {"body": {"name": "test-model", "key": "to-delete"}}


@patch("databricks_cdk.resources.mlflow.registered_model._create_registered_model")
def test_create_or_update_registered_model_new(patched__create_registered_model):
    props = RegisteredModelProperties(
        name="new-model-name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    patched__create_registered_model.return_value = "new-model-name"

    # completely new experiment
    response = create_or_update_registered_model(props, physical_resource_id=None)
    assert response == RegisteredModelCreateResponse(physical_resource_id="new-model-name")


@patch("databricks_cdk.resources.mlflow.registered_model._get_registered_model")
def test_create_or_update_registered_model_existing(
    patched__get_existing_registered_model,
):
    props = RegisteredModelProperties(
        name="model-name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    patched__get_existing_registered_model.return_value = RegisteredModel(
        name="model-name",
        last_updated_timestamp=1234,
        creation_timestamp=1234,
        description="same description",
    )

    response = create_or_update_registered_model(props, physical_resource_id="model-name")

    assert patched__get_existing_registered_model.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models",
        "model-name",
    )

    assert response == RegisteredModelCreateResponse(physical_resource_id="model-name")

    # this is invalid and should raise error
    patched__get_existing_registered_model.return_value = None
    with pytest.raises(ValueError):
        create_or_update_registered_model(props, physical_resource_id="model-name")


@patch("databricks_cdk.resources.mlflow.registered_model.delete_request")
def test_delete_experiment(patched_delete_request):
    props = RegisteredModelProperties(
        name="name",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    delete_registered_model(props, "name")
    assert patched_delete_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/registered-models/delete",
    )

    assert patched_delete_request.call_args.kwargs == {"body": {"name": "name"}}
