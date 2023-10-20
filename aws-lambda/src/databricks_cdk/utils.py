import logging
import os
from typing import Any, Dict, Optional

import boto3
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from pydantic import BaseModel
from requests import request
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from tenacity import retry, retry_if_exception, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


USER_PARAM = os.environ.get("USER_PARAM", "/databricks/deploy/user")
PASS_PARAM = os.environ.get("PASS_PARAM", "/databricks/deploy/password")
ACCOUNT_PARAM = os.environ.get("ACCOUNT_PARAM", "/databricks/account-id")

ACCOUNTS_BASE_URL = os.environ.get("BASE_URL", "https://accounts.cloud.databricks.com")


def get_param(name: str, required: bool = False):
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(
        Name=name,
        WithDecryption=True,
    )
    result = response.get("Parameter", {}).get("Value")
    if result is None and required:
        raise AttributeError(f"Parameter '{name}' not found")
    return result


class CnfResponse(BaseModel):
    physical_resource_id: str


def get_account_id() -> str:
    """Get databricks account id from param store"""
    return get_param(ACCOUNT_PARAM, required=True)


def get_deploy_user() -> str:
    return get_param(USER_PARAM, required=True)


def get_password() -> str:
    return get_param(PASS_PARAM, required=True)


def get_auth() -> HTTPBasicAuth:
    """Get auth from param store"""
    user = get_deploy_user()
    password = get_param(PASS_PARAM, required=True)
    return HTTPBasicAuth(user, password)


@retry(
    retry=retry_if_exception_type(HTTPError) & retry_if_exception(lambda ex: ex.response.status_code == 429),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def _do_request(
    method: str,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generic method to do any type of request

    :param method: Request method to use when doing a request
    :param url: Url to which to make the request
    :param body: Optional body to send along with the request, defaults to None
    :param params: Optional params to send along with the request, defaults to None
    :param auth: Auth to use which is injected when not explicitely overriden, defaults to get_auth()
    :raises ValueError: If provided method is not supported
    :return: Response data
    """
    auth = get_auth()
    resp = request(method=method, url=url, json=body, params=params, auth=auth)

    # If the response was successful, no Exception will be raised
    if resp.status_code >= 400:
        logger.warning(resp.text)
    resp.raise_for_status()

    return resp.json()


def post_request(
    url: str,
    body: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generic method to do post requests"""
    return _do_request(method="POST", url=url, body=body, params=params)


def put_request(
    url: str,
    body: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generic method to do put requests"""
    return _do_request(method="PUT", url=url, body=body, params=params)


def patch_request(url: str, body: dict, params: dict = None) -> dict:
    """Generic method to do patch requests"""

    return _do_request(method="PATCH", url=url, body=body, params=params)


def get_request(
    url: str,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generic method to do get requests"""
    return _do_request(method="GET", url=url, body=body, params=params)


def delete_request(
    url: str,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generic method to do delete requests"""
    return _do_request(method="DELETE", url=url, body=body, params=params)


def get_workspace_client(workspace_url: str, config: Optional[Config] = None) -> WorkspaceClient:
    """Get Databricks WorkspaceClient instance, either from config or from username/password
    :param workspace_url: Workspace url to connect to
    :param config: Optional config to use, when provided overwrites workspace_url provided,
        defaults to None
    """
    if config:
        return WorkspaceClient(config=config)

    return WorkspaceClient(username=get_deploy_user(), password=get_password(), host=workspace_url)
