from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError
from requests.models import Response

from databricks_cdk.utils import _do_request, delete_request, get_request, patch_request, post_request, put_request


@patch("databricks_cdk.utils.get_auth")
@patch("databricks_cdk.utils.request")
def test__do_request_success(patched_requests, patched_get_auth):
    # Prepare
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 200
    expected_response = {"result": "success"}
    mock_response.json.return_value = expected_response
    mock_response

    patched_get_auth.return_value = {}

    patched_requests.return_value = mock_response
    expected_url = "https://example.com"
    expected_body = {"key": "value"}
    expected_params = {"param": "value"}

    # Execute POST
    result = _do_request("POST", expected_url, body=expected_body, params=expected_params)

    # Verify
    patched_requests.assert_called_once_with(
        method="POST", url=expected_url, json=expected_body, params=expected_params, auth={}
    )
    mock_response.raise_for_status.assert_called_once()
    assert result == expected_response


@patch("databricks_cdk.utils.get_auth")
@patch("databricks_cdk.utils.request")
def test__do_request_http_error_retry(patched_requests, patched_get_auth):
    # Prepare
    _do_request.retry.sleep = MagicMock()  # remove sleep in between
    expected_url = "https://example.com"
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 429
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)

    patched_requests.return_value = mock_response
    # Execute
    with pytest.raises(HTTPError):
        _do_request("POST", expected_url)

    # Verify
    assert patched_requests.call_count == 5  # Retried 5 times


@patch("databricks_cdk.utils.get_auth")
@patch("databricks_cdk.utils.request")
def test__do_request_http_error_no_retry(patched_requests, patched_get_auth):
    # Prepare
    expected_url = "https://example.com"
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)

    patched_requests.return_value = mock_response
    # Execute
    with pytest.raises(HTTPError):
        _do_request("POST", expected_url)

    # Verify
    assert patched_requests.call_count == 1  # Not retried


@patch("databricks_cdk.utils.get_auth")
def test__do_request_unsupported_method(patched_get_auth):
    # Prepare
    expected_url = "https://example.com"

    # Execute and Verify
    with pytest.raises(HTTPError):
        _do_request("INVALID_METHOD", expected_url)


@patch("databricks_cdk.utils._do_request")
def test_post_request(patched__do_request):
    expected_url = "test.com"
    expected_body = {"test", "value"}
    post_request(expected_url, expected_body)

    patched__do_request.assert_called_once_with(method="POST", url=expected_url, body=expected_body, params=None)


@patch("databricks_cdk.utils._do_request")
def test_put_request(patched__do_request):
    expected_url = "test.com"
    expected_body = {"test", "value"}
    put_request(expected_url, expected_body)

    patched__do_request.assert_called_once_with(method="PUT", url=expected_url, body=expected_body, params=None)


@patch("databricks_cdk.utils._do_request")
def test_patch_request(patched__do_request):
    expected_url = "test.com"
    expected_body = {"test", "value"}
    patch_request(expected_url, expected_body)

    patched__do_request.assert_called_once_with(method="PATCH", url=expected_url, body=expected_body, params=None)


@patch("databricks_cdk.utils._do_request")
def test_get_request(patched__do_request):
    expected_url = "test.com"
    expected_body = {"test", "value"}
    get_request(expected_url, expected_body)

    patched__do_request.assert_called_once_with(method="GET", url=expected_url, body=expected_body, params=None)


@patch("databricks_cdk.utils._do_request")
def test_delete_request(patched__do_request):
    expected_url = "test.com"
    expected_body = {"test", "value"}
    delete_request(expected_url, expected_body)

    patched__do_request.assert_called_once_with(method="DELETE", url=expected_url, body=expected_body, params=None)
