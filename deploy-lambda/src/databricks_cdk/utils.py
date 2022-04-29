import logging
import os
from typing import Optional

import boto3
import requests
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


USER_PARAM = os.environ.get("USER_PARAM", "/databricks/deploy/user")
PASS_PARAM = os.environ.get("PASS_PARAM", "/databricks/deploy/password")
ACCOUNT_PARAM = os.environ.get("PASS_PARAM", "/databricks/account-id")

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


def get_account_id():
    """Get databricks account id from param store"""
    return get_param(ACCOUNT_PARAM, required=True)


def get_deploy_user():
    return get_param(USER_PARAM, required=True)


def get_auth() -> HTTPBasicAuth:
    """Get auth from param store"""
    user = get_deploy_user()
    password = get_param(PASS_PARAM, required=True)
    return HTTPBasicAuth(user, password)


def post_request(
    url: str,
    body: dict,
) -> dict:
    """Generic method to do post requests"""
    auth = get_auth()
    resp = requests.post(url, json=body, headers={}, auth=auth)

    # If the response was successful, no Exception will be raised
    if resp.status_code >= 400:
        logger.warning(resp.text)
    resp.raise_for_status()

    # extracting data in json format
    data = resp.json()

    return data


def put_request(
    url: str,
    body: dict,
) -> dict:
    """Generic method to do post requests"""
    auth = get_auth()
    resp = requests.put(url, json=body, headers={}, auth=auth)

    # If the response was successful, no Exception will be raised
    if resp.status_code >= 400:
        logger.warning(resp.text)
    resp.raise_for_status()

    # extracting data in json format
    data = resp.json()

    return data


def patch_request(url: str, body: dict) -> dict:
    """Generic method to do patch requests"""
    auth = get_auth()
    resp = requests.patch(url, json=body, headers={}, auth=auth)

    # If the response was successful, no Exception will be raised
    if resp.status_code >= 400:
        logger.warning(resp.text)
    resp.raise_for_status()

    # extracting data in json format
    data = resp.json()

    return data


def get_request(url: str, params: dict = None, body: dict = None) -> Optional[dict]:
    """Generic method to do get requests"""
    auth = get_auth()
    resp = requests.get(url, headers={}, auth=auth, params=params, json=body)

    if resp.status_code == 404:
        return None

    # If the response was successful, no Exception will be raised
    if resp.status_code >= 400:
        logger.warning(resp.text)
    resp.raise_for_status()

    # extracting data in json format
    data = resp.json()

    logger.debug("Successful GET call!!")
    return data


def delete_request(url: str) -> Optional[dict]:
    """Generic method to do delete requests"""
    auth = get_auth()
    resp = requests.delete(url, headers={}, auth=auth)

    # If the response was successful, no Exception will be raised
    if resp.status_code >= 400:
        logger.warning(resp.text)
    resp.raise_for_status()

    # extracting data in json format
    data = resp.json()

    return data
