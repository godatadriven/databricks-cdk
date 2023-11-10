from unittest.mock import patch

import pytest
from databricks.sdk.service.ml import CreateExperimentResponse, Experiment, ExperimentTag, GetExperimentResponse

from databricks_cdk.resources.mlflow.experiment import (
    CnfResponse,
    ExperimentCreateResponse,
    ExperimentIdNoneError,
    ExperimentProperties,
    create_or_update_experiment,
    delete_experiment,
)


@patch("databricks_cdk.resources.mlflow.experiment.get_workspace_client")
def test_create_or_update_experiment_new(patched_get_workspace_client, workspace_client):
    patched_get_workspace_client.return_value = workspace_client
    workspace_client.experiments.create_experiment.return_value = CreateExperimentResponse(
        experiment_id="some_experiment_id"
    )
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )

    # completely new experiment
    response = create_or_update_experiment(props, physical_resource_id=None)
    assert response == ExperimentCreateResponse(physical_resource_id="some_experiment_id", name="name")
    workspace_client.experiments.create_experiment.assert_called_once_with(
        name="name",
        artifact_location="s3://test",
        tags=[ExperimentTag(key="mlflow.note.content", value="same description")],
    )


@patch("databricks_cdk.resources.mlflow.experiment.get_workspace_client")
def test_create_or_update_experiment_existing(patched_get_workspace_client, workspace_client):
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    patched_get_workspace_client.return_value = workspace_client
    workspace_client.experiments.get_experiment.return_value = GetExperimentResponse(
        experiment=Experiment(
            experiment_id="test_id",
            name="old_name",
            artifact_location="s3://test",
            lifecycle_stage="blah",
            last_update_time=1234,
            creation_time=1234,
            tags=[ExperimentTag(key="mlflow.note.content", value="some old description")],
        )
    )

    # already existing experiment
    response = create_or_update_experiment(props, physical_resource_id="test_id")

    assert response == ExperimentCreateResponse(physical_resource_id="test_id", name="name")
    workspace_client.experiments.update_experiment.assert_called_once_with(experiment_id="test_id", new_name="name")


@patch("databricks_cdk.resources.mlflow.experiment.get_workspace_client")
def test_create_or_update_experiment_existing_invalid_id(patched_get_workspace_client, workspace_client):
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    patched_get_workspace_client.return_value = workspace_client
    workspace_client.experiments.get_experiment.return_value = GetExperimentResponse(experiment=None)

    # already existing experiment
    with pytest.raises(ExperimentIdNoneError):
        create_or_update_experiment(props, physical_resource_id="test_id")


@patch("databricks_cdk.resources.mlflow.experiment.get_workspace_client")
def test_delete_experiment(patched_get_workspace_client, workspace_client):
    patched_get_workspace_client.return_value = workspace_client
    props = ExperimentProperties(
        name="name",
        artifact_location="s3://test",
        workspace_url="https://test.cloud.databricks.com",
        description="same description",
    )
    response = delete_experiment(props, "some_id")
    assert response == CnfResponse(physical_resource_id="some_id")
    workspace_client.experiments.delete_experiment.assert_called_once_with(experiment_id="some_id")


# TODO: Add test for tag updates
