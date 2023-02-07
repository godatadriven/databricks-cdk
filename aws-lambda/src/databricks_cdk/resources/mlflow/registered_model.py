from enum import Enum
from typing import List, Optional

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, delete_request, get_request, patch_request, post_request


class RegisteredModelTag(BaseModel):
    key: str
    value: str


class ModelVersionTag(BaseModel):
    key: str
    value: str


class RegisteredModelProperties(BaseModel):
    name: str
    tags: List[RegisteredModelTag] = []
    description: Optional[str]
    workspace_url: str


class ModelVersionStatus(str, Enum):
    PENDING_REGISTRATION = "PENDING_REGISTRATION"
    FAILED_REGISTRATION = "FAILED_REGISTRATION"
    READY = "READY"


class ModelVersion(BaseModel):
    name: str
    version: str
    creation_timestamp: int
    last_updated_timestamp: int
    user_id: str
    current_stage: str
    description: Optional[str]
    source: str
    run_id: str
    status: ModelVersionStatus
    status_message: Optional[str]
    tags: Optional[List[ModelVersionTag]]
    run_link: Optional[str]


class RegisteredModel(BaseModel):
    name: str
    creation_timestamp: int
    last_updated_timestamp: int
    description: Optional[str]
    latest_versions: Optional[List[ModelVersion]]
    tags: Optional[List[RegisteredModelTag]]
    user_id: Optional[str]  # Currently not returned


class RegisteredModelCreateResponse(CnfResponse):
    physical_resource_id: str


def get_registered_model_url(workspace_url: str):
    """Get the mlflow registered-models url"""
    return f"{workspace_url}/api/2.0/mlflow/registered-models"


def _create_registered_model(registered_model_url: str, properties: RegisteredModelProperties) -> str:
    """Creates a registered model"""
    response = post_request(
        f"{registered_model_url}/create",
        {
            "name": properties.name,
            "tags": [{"key": t.key, "value": t.value} for t in properties.tags],
            "description": properties.description,
        },
    )
    return response["registered_model"]["name"]


def _get_registered_model(registered_model_url: str, name: str) -> Optional[RegisteredModel]:
    """Gets the registered model"""
    response = get_request(f"{registered_model_url}/get?name={name}")
    if response:
        return RegisteredModel.parse_obj(response["registered_model"])

    return None


def _update_registered_model_description(registered_model_url: str, registered_model_name: str, description: str):
    """Updates the registered model description"""
    return patch_request(
        f"{registered_model_url}/update",
        body={"name": registered_model_name, "description": description},
    )


def _update_registered_model_name(registered_model_url: str, current_name: str, new_name: str) -> str:
    """Updates the registered model name"""
    return post_request(f"{registered_model_url}/rename", {"name": current_name, "new_name": new_name})[
        "registered_model"
    ]["name"]


def _update_registered_model_tags(
    registered_model_url: str,
    properties: RegisteredModelProperties,
    current_tags: List[RegisteredModelTag],
):
    """Updates the registered model tags"""
    tags_to_delete = []

    if not properties.tags:
        tags_to_delete = current_tags
    elif current_tags is not None:
        new_keys = [t.key for t in properties.tags]
        tags_to_delete = [t for t in current_tags if t.key not in new_keys]

    if tags_to_delete:
        [
            delete_request(
                f"{registered_model_url}/delete-tag",
                body={"name": properties.name, "key": t.key},
            )
            for t in tags_to_delete
        ]

    if properties.tags:
        # Overwrites / updates existing tags
        [
            post_request(
                f"{registered_model_url}/set-tag",
                {"name": properties.name, "key": t.key, "value": t.value},
            )
            for t in properties.tags
        ]


def create_or_update_registered_model(
    properties: RegisteredModelProperties, physical_resource_id: Optional[str] = None
):
    """
    Creates a new registered model if no physical_resource_id is provided. Otherwise, this function checks whether the
    registered model needs to be updated based on the provided properties.

    :param properties: Properties of an mlflow Registered Model
    :param physical_resource_id: CDK Physical Resource Id belonging to the Registered Model (if exists). Defaults to None
    :return:physical_resource_id of the Registered Model, which equals the name of the Registered Model
    """
    registered_model_url = get_registered_model_url(properties.workspace_url)

    if not physical_resource_id:
        registered_model_name = _create_registered_model(registered_model_url, properties)
        return RegisteredModelCreateResponse(physical_resource_id=registered_model_name)

    registered_model_url = get_registered_model_url(properties.workspace_url)
    registered_model = _get_registered_model(registered_model_url, physical_resource_id)

    if not registered_model:
        raise ValueError(f"Registered model cannot be found but physical_resouce_id {physical_resource_id} is provided")

    if properties.name != registered_model.name:
        physical_resource_id = _update_registered_model_name(
            registered_model_url,
            current_name=physical_resource_id,
            new_name=properties.name,
        )

    if properties.description != registered_model.description:
        _update_registered_model_description(registered_model_url, physical_resource_id, properties.description)

    new_tags = properties.tags
    if properties.tags is not None:
        new_tags = sorted(new_tags, key=lambda t: t.key)

    current_tags = registered_model.tags
    if current_tags is not None:
        current_tags = sorted(current_tags, key=lambda t: t.key)

    if new_tags != current_tags:
        _update_registered_model_tags(registered_model_url, properties, registered_model.tags)
    return RegisteredModelCreateResponse(physical_resource_id=physical_resource_id)


def delete_registered_model(properties: RegisteredModelProperties, physical_resource_id: str):
    """Deletes an existing registered model"""
    delete_request(
        f"{get_registered_model_url(properties.workspace_url)}/delete",
        body={"name": physical_resource_id},
    )
    return CnfResponse(physical_resource_id=physical_resource_id)
