import logging
from typing import Optional

from pydantic import BaseModel

from databricks_cdk.utils import (
    ACCOUNTS_BASE_URL,
    CnfResponse,
    delete_request,
    get_account_id,
    get_request,
    post_request,
)

logger = logging.getLogger(__name__)


class CredentialsProperties(BaseModel):
    action: str = "credentials"
    credentials_name: str
    role_arn: str


class CredentialsResponse(CnfResponse):
    credentials_name: str
    credentials_id: str
    role_arn: str
    external_id: str
    creation_time: int


def get_credentials_url():
    """Getting url for credentials requests"""
    account_id = get_account_id()

    # api-endpoint
    return f"{ACCOUNTS_BASE_URL}/api/2.0/accounts/{account_id}/credentials"


def get_credentials_by_id(credentials_id: str) -> Optional[dict]:
    URL = get_credentials_url()
    return get_request(url=f"{URL}/{credentials_id}")


def get_credentials_by_name(credentials_name: str) -> Optional[dict]:
    """Getting credentials based on name"""
    URL = get_credentials_url()
    get_response = get_request(url=URL)
    current = None
    for r in get_response:
        if r.get("credentials_name") == credentials_name:
            current = r
    return current


def create_or_update_credentials(properties: CredentialsProperties) -> CredentialsResponse:
    """Create credentials config at databricks"""
    url = get_credentials_url()

    current = get_credentials_by_name(properties.credentials_name)
    if current is None:

        # Json data
        body = {
            "credentials_name": properties.credentials_name,
            "aws_credentials": {"sts_role": {"role_arn": properties.role_arn}},
        }
        response = post_request(url, body=body)
        external_id = response.get("aws_credentials", {}).get("sts_role", "").get("external_id")
        return CredentialsResponse(
            physical_resource_id=response["credentials_id"],
            credentials_name=properties.credentials_name,
            credentials_id=response["credentials_id"],
            role_arn=properties.role_arn,
            external_id=external_id,
            creation_time=response["creation_time"],
        )
    else:
        current_role_arn = current.get("aws_credentials", {}).get("sts_role", "").get("role_arn")
        external_id = current.get("aws_credentials", {}).get("sts_role", "").get("external_id")
        if current_role_arn != properties.role_arn:
            raise AttributeError("Role arn can't be changed after deployment")
        return CredentialsResponse(
            physical_resource_id=current["credentials_id"],
            credentials_name=properties.credentials_name,
            credentials_id=current["credentials_id"],
            role_arn=properties.role_arn,
            external_id=external_id,
            creation_time=current["creation_time"],
        )


def delete_credentials(properties: CredentialsProperties, physical_resource_id: str) -> CnfResponse:
    """Deletes credentials config at databricks"""
    URL = get_credentials_url()

    current = get_credentials_by_id(physical_resource_id)
    if current is None:
        current = get_credentials_by_name(properties.credentials_name)
    if current is not None:
        credentials_id = current["credentials_id"]
        delete_request(f"{URL}/{credentials_id}")
    else:
        logger.warning("Already removed")
    return CnfResponse(physical_resource_id=properties.credentials_name)
