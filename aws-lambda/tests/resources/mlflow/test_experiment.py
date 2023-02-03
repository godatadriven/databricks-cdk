from unittest.mock import patch

import pytest

from databricks_cdk.resources.mlflow.experiment import (
    ExperimentCreateResponse,
    ExperimentExisting,
    ExperimentProperties,
    ExperimentTag,
    _create_experiment,
    _get_existing_experiment,
    _update_experiment,
    _update_experiment_description,
    _update_experiment_name,
    create_or_update_experiment,
    delete_experiment,
    get_experiment_url,
)


def test_get_experiment_url():
    workspace_url = "https://test.cloud.databricks.com"
    assert get_experiment_url(workspace_url) == "https://test.cloud.databricks.com/api/2.0/mlflow/experiments"


@patch("databricks_cdk.resources.mlflow.experiment.post_request")
def test__create_experiment(patched_post_request):
    props = ExperimentProperties(
        name="test",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="some description",
    )
    patched_post_request.return_value = {"experiment_id": "test_id"}
    experiment_id = _create_experiment("https://test.cloud.databricks.com/api/2.0/mlflow/experiments", props)

    assert experiment_id == "test_id"
    assert patched_post_request.call_count == 1

    assert patched_post_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments/create",
        {
            "name": "test",
            "artifact_location": "s3://test",
            "tags": {"key": "mlflow.note.content", "value": "some description"},
        },
    )


@patch("databricks_cdk.resources.mlflow.experiment.post_request")
def test__update_experiment_name(patched_post_request):
    _update_experiment_name(
        experiment_url="https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        experiment_id="test_id",
        new_name="new_name",
    )

    assert patched_post_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments/update",
        {"experiment_id": "test_id", "new_name": "new_name"},
    )


@patch("databricks_cdk.resources.mlflow.experiment.post_request")
def test__update_experiment_description(patched_post_request):
    _update_experiment_description(
        experiment_url="https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        experiment_id="test_id",
        new_description="new_description",
    )

    assert patched_post_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments/set-experiment-tag",
        {"experiment_id": "test_id", "key": "mlflow.note.content", "value": "new_description"},
    )


@patch("databricks_cdk.resources.mlflow.experiment._update_experiment_name")
@patch("databricks_cdk.resources.mlflow.experiment._update_experiment_description")
def test__update_experiment_changes(
    patched__updated_experiment_description,
    patched__update_experiment_name,
):
    props = ExperimentProperties(
        name="new_name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="some new description",
    )

    existing_experiment = ExperimentExisting(
        experiment_id="test_id",
        name="old_name",
        artifact_location="s3://test",
        lifecycle_stage="blah",
        last_update_time=1234,
        creation_time=1234,
        tags=[ExperimentTag(key="mlflow.note.content", value="some old description")],
    )

    # both name update and description update should be called
    _update_experiment(
        experiment_url="https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        properties=props,
        existing_experiment=existing_experiment,
    )

    assert patched__update_experiment_name.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        "test_id",
        "new_name",
    )

    assert patched__updated_experiment_description.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        "test_id",
        "some new description",
    )


@patch("databricks_cdk.resources.mlflow.experiment._update_experiment_name")
@patch("databricks_cdk.resources.mlflow.experiment._update_experiment_description")
def test__update_experiment_no_changes(
    patched__updated_experiment_description,
    patched__update_experiment_name,
):
    props = ExperimentProperties(
        name="same_name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    existing_experiment = ExperimentExisting(
        experiment_id="test_id",
        name="same_name",
        artifact_location="s3://test",
        lifecycle_stage="blah",
        last_update_time=1234,
        creation_time=1234,
        tags=[ExperimentTag(key="mlflow.note.content", value="same description")],
    )

    # both name update and description update should be called
    _update_experiment(
        experiment_url="https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        properties=props,
        existing_experiment=existing_experiment,
    )

    # update functions should not have been called
    patched__updated_experiment_description.call_count == 0
    patched__update_experiment_name.call_count == 0


@patch("databricks_cdk.resources.mlflow.experiment.get_request")
def test__get_existing_experiment(patched__get_request):
    props = ExperimentProperties(
        name="same_name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    patched__get_request.return_value = {
        "experiment": {
            "experiment_id": "1234",
            "name": "same_name",
            "artifact_location": "s3://test",
            "lifecycle_stage": "blah",
            "last_update_time": 1234,
            "creation_time": 1234,
            "tags": [{"key": "mlflow.note.content", "value": "same description"}],
        }
    }

    existing_experiment = _get_existing_experiment(
        experiment_url="https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        properties=props,
        experiment_id="1234",
    )

    assert isinstance(existing_experiment, ExperimentExisting)

    patched__get_request.return_value = None
    assert not _get_existing_experiment(
        experiment_url="https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        properties=props,
        experiment_id="1234",
    )


@patch("databricks_cdk.resources.mlflow.experiment._create_experiment")
def test_create_or_update_experiment_new(patched__create_experiment):
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    patched__create_experiment.return_value = "some_experiment_id"

    # completely new experiment
    response = create_or_update_experiment(props, physical_resource_id=None)
    assert response == ExperimentCreateResponse(physical_resource_id="some_experiment_id", name="name")


@patch("databricks_cdk.resources.mlflow.experiment._get_existing_experiment")
@patch("databricks_cdk.resources.mlflow.experiment._update_experiment")
def test_create_or_update_experiment_existing(patched__update_experiment, patched__get_existing_experiment):
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    patched__get_existing_experiment.return_value = ExperimentExisting(
        experiment_id="test_id",
        name="old_name",
        artifact_location="s3://test",
        lifecycle_stage="blah",
        last_update_time=1234,
        creation_time=1234,
        tags=[ExperimentTag(key="mlflow.note.content", value="some old description")],
    )

    # already existing experiment
    response = create_or_update_experiment(props, physical_resource_id="test_id")

    assert patched__update_experiment.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        ExperimentProperties(
            name="name",
            artifact_location="s3://test",
            workspace_url="https://test.cloud.databricks.com",
            description="same description",
        ),
        ExperimentExisting(
            experiment_id="test_id",
            name="old_name",
            artifact_location="s3://test",
            lifecycle_stage="blah",
            last_update_time=1234,
            creation_time=1234,
            tags=[ExperimentTag(key="mlflow.note.content", value="some old description")],
        ),
    )

    assert patched__get_existing_experiment.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments",
        ExperimentProperties(
            name="name",
            artifact_location="s3://test",
            workspace_url="https://test.cloud.databricks.com",
            description="same description",
        ),
        "test_id",
    )

    assert response == ExperimentCreateResponse(physical_resource_id="test_id", name="name")

    # this is invalid and should raise error
    patched__get_existing_experiment.return_value = None
    with pytest.raises(ValueError):
        response = create_or_update_experiment(props, physical_resource_id="test_id")


@patch("databricks_cdk.resources.mlflow.experiment.post_request")
def test_delete_experiment(patched_post_request):
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    delete_experiment(props, "some_id")
    assert patched_post_request.call_args.args == (
        "https://test.cloud.databricks.com/api/2.0/mlflow/experiments/delete",
        {"experiment_id": "some_id"},
    )
