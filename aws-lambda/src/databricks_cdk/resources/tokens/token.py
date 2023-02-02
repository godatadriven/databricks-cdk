import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from databricks_cdk.utils import CnfResponse, get_request, post_request

logger = logging.getLogger(__name__)


class TokenInfo(BaseModel):
    token_id: str
    creation_time: int
    expiry_time: int
    comment: str


class TokenList(BaseModel):
    token_infos: List[TokenInfo]


class TokenProperties(BaseModel):
    action: str = "token"
    workspace_url: str
    lifetime_seconds: Optional[int]
    comment: Optional[str]


class TokenCreateReponse(CnfResponse):
    token_value = str
    creation_time: int
    expiry_time: int
    comment: str


def get_existing_tokens(token_url: str) -> List[TokenInfo]:
    """Get a list of existing tokens"""
    response = get_request(f"{token_url}/list")

    if response:
        return TokenList.parse_obj(response).token_infos

    return []


def get_token_url(workspace_url: str):
    """Getting url for token requests"""
    return f"{workspace_url}/api/2.0/token"


def _delete_token(token_url: str, token_id: str):
    post_request(url=f"{token_url}/delete", body={"token_id": token_id})


def _create_token(token_url: str, comment: Optional[str], lifetime_seconds: Optional[int]) -> Dict[str, Any]:
    return post_request(f"{token_url}/create", body={"comment": comment, "lifetime_seconds": comment})


def create_token(properties: TokenProperties, physical_resource_id: Optional[str] = None) -> CnfResponse:
    """
    Create a new token. Everytime this function is called a new token is created returning a
    new physical_resource_id. This is because we cannot retrieve the value of an already existing token_value

    :param properties: Properties to use when creating token
    :param physical_resource_id: Physical resource id of previous token object
    :return: Cloudformation response
    """
    url = get_token_url(properties.workspace_url)

    # delete previous token before creating a new one
    if physical_resource_id in [t.token_id for t in get_existing_tokens(url)]:
        _delete_token(url, physical_resource_id)

    response = _create_token(url, properties.comment, properties.lifetime_seconds)

    return TokenCreateReponse(
        physical_resource_id=response["token_info"]["token_id"],
        token_value=response["token_value"],
        creation_time=response["token_info"]["creation_time"],
        expiry_time=response["token_info"]["expiry_time"],
        comment=response["token_info"]["comment"],
    )


def delete_token(properties: TokenProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes token from databricks"""

    url = get_token_url(properties.workspace_url)

    if physical_resource_id in [t.token_id for t in get_existing_tokens(url)]:
        _delete_token(token_url=url, token_id=physical_resource_id)

    return CnfResponse(physical_resource_id=physical_resource_id)
