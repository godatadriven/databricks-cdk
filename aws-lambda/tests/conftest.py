import os
from unittest.mock import MagicMock

import pytest
from databricks.sdk import ExperimentsAPI, ModelRegistryAPI, VolumesAPI, WorkspaceClient


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Generate fake AWS environment variables so boto3 is never called with 'real' values"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"


@pytest.fixture(scope="function")
def workspace_client():
    workspace_client = MagicMock(spec=WorkspaceClient)

    # mock all of the underlying service api's
    workspace_client.model_registry = MagicMock(spec=ModelRegistryAPI)
    workspace_client.experiments = MagicMock(spec=ExperimentsAPI)
    workspace_client.volumes = MagicMock(spec=VolumesAPI)

    return workspace_client
