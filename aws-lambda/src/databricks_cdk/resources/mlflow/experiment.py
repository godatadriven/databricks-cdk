from typing import List, Optional

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, get_request, post_request


class ExperimentTag(BaseModel):
    key: str
    value: str


class ExperimentProperties(BaseModel):
    name: str
    artifact_location: Optional[str] = None
    workspace_url: str
    description: Optional[str] = None


class ExperimentCreateResponse(CnfResponse):
    physical_resource_id: str
    name: str


class ExperimentExisting(BaseModel):
    experiment_id: str
    name: str
    artifact_location: str
    lifecycle_stage: str
    last_update_time: int
    creation_time: int
    tags: List[ExperimentTag]


def get_experiment_url(workspace_url: str) -> str:
    """Get the mlflow experiment url"""
    return f"{workspace_url}/api/2.0/mlflow/experiments"


def _create_experiment(experiment_url: str, properties: ExperimentProperties) -> str:
    """Creates a new experiment"""
    return post_request(
        f"{experiment_url}/create",
        {
            "name": properties.name,
            "artifact_location": properties.artifact_location,
            "tags": {"key": "mlflow.note.content", "value": properties.description},
        },
    )["experiment_id"]


def _update_experiment_name(experiment_url: str, experiment_id: str, new_name: str):
    """Updates the experiment name"""
    post_request(
        f"{experiment_url}/update",
        {"experiment_id": experiment_id, "new_name": new_name},
    )


def _update_experiment_description(experiment_url: str, experiment_id: str, new_description: str):
    """Updates the description of the experiment which is a fixed key of the experiment tags (mlflow.note.content)"""
    post_request(
        f"{experiment_url}/set-experiment-tag",
        {
            "experiment_id": experiment_id,
            "key": "mlflow.note.content",
            "value": new_description,
        },
    )


def _update_experiment(experiment_url: str, properties: ExperimentProperties, existing_experiment: ExperimentExisting):
    """Updates an experiment if there is a new name or a new description"""
    if properties.name != existing_experiment.name:
        _update_experiment_name(experiment_url, existing_experiment.experiment_id, properties.name)

    new_description = ExperimentTag(key="mlflow.note.content", value=properties.description)
    if new_description not in existing_experiment.tags:
        _update_experiment_description(
            experiment_url,
            existing_experiment.experiment_id,
            new_description.value,
        )


def _get_existing_experiment(
    experiment_url: str, properties: ExperimentProperties, experiment_id: Optional[str]
) -> Optional[ExperimentExisting]:
    """Gets an existing experiment from mlflow based on experiment_id"""
    existing_experiment = get_request(f"{experiment_url}/get?experiment_id={experiment_id}")

    if existing_experiment:
        return ExperimentExisting.parse_obj(existing_experiment["experiment"])

    return None


def create_or_update_experiment(properties: ExperimentProperties, physical_resource_id: Optional[str] = None):
    """
    Creates a new experiment if there is no physical_resource_id provided, else it checks if based on the
    ExperimentProperties the experiment can be updated


    :param properties: Properties of an mlflow experiment
    :param physical_resource_id: CDK physical resource id which is 1 on 1 mapped to experiment_id,
        defaults to None
    :return: Both the physical_resouce_id (experiment_id) and the name of the experiment
    """
    experiment_url = get_experiment_url(properties.workspace_url)

    if not physical_resource_id:
        experiment_id = _create_experiment(experiment_url, properties)
        return ExperimentCreateResponse(name=properties.name, physical_resource_id=experiment_id)

    existing_experiment = _get_existing_experiment(experiment_url, properties, physical_resource_id)

    if not existing_experiment:
        raise ValueError("Existing experiment cannot be found but physical_resouce_id is provided")

    _update_experiment(experiment_url, properties, existing_experiment)
    return ExperimentCreateResponse(name=properties.name, physical_resource_id=physical_resource_id)


def delete_experiment(properties: ExperimentProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes an existing mlflow experiment"""
    post_request(f"{get_experiment_url(properties.workspace_url)}/delete", {"experiment_id": physical_resource_id})
    return CnfResponse(physical_resource_id=physical_resource_id)
