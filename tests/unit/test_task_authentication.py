import json
import logging
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.reader.api.auth.azure import AzureOauthTask
from data_platform.tasks.reader.api.auth.basic import BasicOauthTask
from data_platform.tasks.reader.api.auth.orca import OrcaOauthTask


@pytest.fixture(scope="session")
def setup_azure_oauth_task_config() -> Configuration:
    """Fixture to provide the configuration for AzureOauthTask."""
    return Configuration(
        {
            "1": {
                "task_name": "AzureOauthTask",
                "namespace": "api_credentials",
                "tenant_id_key": "tenant-id",
                "client_id_key": "client-id",
                "client_secret_key": "client-secret",
                "api_scope_key": "api-scope",
                "access_token_key": "access_token",
            }
        }
    )


@patch("requests.post")
def test_azure_oauth_task_token_retrieval(
    mock_post: Mock, spark: SparkSession, logger: logging.Logger, setup_azure_oauth_task_config: Configuration
) -> None:
    """Test that AzureOauthTask retrieves secrets and requests a token correctly."""
    # Configure mock response for token request
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"access_token": "mocked-access-token"}

    # Set up TaskContext and configuration
    task_context = TaskContext(logger=logger, spark=spark, configuration=setup_azure_oauth_task_config)
    task_context.put_property("api_credentials", "client-id", "test-client-id")
    task_context.put_property("api_credentials", "client-secret", "test-client-secret")
    task_context.put_property("api_credentials", "tenant-id", "test-tenant-id")
    task_context.put_property("api_credentials", "api-scope", "https://api.example.com/.default")

    # Run the task
    etl_job = AzureOauthTask()
    etl_job.execute(context=task_context, conf=setup_azure_oauth_task_config.get_tree("1"))

    # Assert the access token was stored in the task context
    stored_token = task_context.get_property("api_credentials", "access_token")
    assert stored_token == "mocked-access-token"

    # Verify that the request was made with the correct data
    expected_data = {
        "grant_type": "client_credentials",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "scope": "https://api.example.com/.default",
    }
    mock_post.assert_called_once_with(
        "https://login.microsoftonline.com/test-tenant-id/oauth2/v2.0/token",
        data=expected_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=True,
    )


@patch("requests.post")
def test_azure_oauth_task_missing_secrets(
    mock_post: Mock, spark: SparkSession, logger: logging.Logger, setup_azure_oauth_task_config: Configuration
) -> None:
    """Test that AzureOauthTask raises an error if secrets are missing."""
    # Set up TaskContext with missing `api-scope`
    task_context = TaskContext(logger=logger, spark=spark, configuration=setup_azure_oauth_task_config)
    task_context.put_property("api_credentials", "client-id", "test-client-id")
    task_context.put_property("api_credentials", "client-secret", "test-client-secret")
    task_context.put_property("api_credentials", "tenant-id", "test-tenant-id")
    task_context.put_property("api_credentials", "api-scope", None)  # Simulate missing scope

    # Run the task and expect a ValueError due to missing secrets
    etl_job = AzureOauthTask()
    with pytest.raises(ValueError, match="Missing required secrets in the task context"):
        etl_job.execute(context=task_context, conf=setup_azure_oauth_task_config.get_tree("1"))


@patch("requests.post")
def test_azure_oauth_task_invalid_response(
    mock_post: Mock, spark: SparkSession, logger: logging.Logger, setup_azure_oauth_task_config: Configuration
) -> None:
    """Test that AzureOauthTask raises an error if the token response is invalid."""
    # Configure mock response without an access token
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {}

    # Set up TaskContext with valid configuration
    task_context = TaskContext(logger=logger, spark=spark, configuration=setup_azure_oauth_task_config)
    task_context.put_property("api_credentials", "client-id", "test-client-id")
    task_context.put_property("api_credentials", "client-secret", "test-client-secret")
    task_context.put_property("api_credentials", "tenant-id", "test-tenant-id")
    task_context.put_property("api_credentials", "api-scope", "https://api.example.com/.default")

    # Run the task and expect a ValueError due to missing access token in response
    etl_job = AzureOauthTask()
    with pytest.raises(ValueError, match="Access token not found in response."):
        etl_job.execute(context=task_context, conf=setup_azure_oauth_task_config.get_tree("1"))


@pytest.fixture(scope="session")
def setup_orca_oauth_task_config() -> Configuration:
    """Fixture to provide the configuration for AzureOauthTask."""
    return Configuration(
        {
            "1": {
                "task_name": "OrcaOauthTask",
                "namespace": "api_credentials",
                "access_token_key": "access_token",
                "username_key": "userkey",
                "password_key": "passwordkey",
                "content_type": "application/json",
                "auth_url": "https://auth.com/token",
                "granttype_key": "client_credentials",
            }
        }
    )


@patch("requests.post")
def test_orca_oauth_task_token_retrieval(
    mock_post: Mock, spark: SparkSession, logger: logging.Logger, setup_orca_oauth_task_config: Configuration
) -> None:
    """Test that OrcaOauthTask retrieves secrets and requests a token correctly."""
    # Configure mock response for token request
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"access_token": "mocked-access-token"}

    # Set up TaskContext and configuration
    task_context = TaskContext(logger=logger, spark=spark, configuration=setup_orca_oauth_task_config)
    task_context.put_property("api_credentials", "userkey", "test-username")
    task_context.put_property("api_credentials", "passwordkey", "test-password")

    # Run the task
    etl_job = OrcaOauthTask()
    etl_job.execute(context=task_context, conf=setup_orca_oauth_task_config.get_tree("1"))

    # Assert the access token was stored in the task context
    stored_token = task_context.get_property("api_credentials", "access_token")
    assert stored_token == "mocked-access-token"

    # Verify that the request was made with the correct data
    expected_data = {"grant_type": "client_credentials", "client_id": "test-username", "client_secret": "test-password"}
    mock_post.assert_called_once_with("https://auth.com/token", data=expected_data, headers=None, verify=True)


@pytest.fixture(scope="session")
def setup_basic_oauth_task_config() -> Configuration:
    """Fixture to provide the configuration for AzureOauthTask."""
    return Configuration(
        {
            "1": {
                "task_name": "BasicOauthTask",
                "namespace": "api_credentials",
                "access_token_key": "access_token",
                "username_key": "userkey",
                "password_key": "passwordkey",
                "content_type": "application/json",
                "auth_url": "https://auth.com/token",
            }
        }
    )


@patch("requests.post")
def test_basic_oauth_task_token_retrieval(
    mock_post: Mock, spark: SparkSession, logger: logging.Logger, setup_basic_oauth_task_config: Configuration
) -> None:
    """Test that BasicOauthTask retrieves secrets and requests a token correctly."""
    # Configure mock response for token request
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"accessToken": "mocked-access-token"}

    # Set up TaskContext and configuration
    task_context = TaskContext(logger=logger, spark=spark, configuration=setup_basic_oauth_task_config)
    task_context.put_property("api_credentials", "userkey", "test-username")
    task_context.put_property("api_credentials", "passwordkey", "test-password")

    # Run the task
    etl_job = BasicOauthTask()
    etl_job.execute(context=task_context, conf=setup_basic_oauth_task_config.get_tree("1"))

    # Assert the access token was stored in the task context
    stored_token = task_context.get_property("api_credentials", "access_token")
    assert stored_token == "mocked-access-token"

    # Verify that the request was made with the correct data
    expected_data = json.dumps({"login": "test-username", "password": "test-password"})
    mock_post.assert_called_once_with(
        "https://auth.com/token", data=expected_data, headers={"Content-Type": "application/json"}, verify=False
    )
