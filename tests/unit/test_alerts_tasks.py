import logging
from unittest.mock import MagicMock, mock_open, patch

import pytest
from pyspark.sql import SparkSession

from data_platform.tasks.alerts.apply_alert_sql import ApplyAlertSQLTask
from data_platform.tasks.alerts.config.dataclasses import ApplyAlertSQLTaskConfig


@pytest.fixture
def task() -> ApplyAlertSQLTask:
    """Fixture to create an instance of ApplyAlertSQLTask."""
    return ApplyAlertSQLTask()


@pytest.fixture
def input_conf() -> ApplyAlertSQLTaskConfig:
    """Fixture to create a configuration for ApplyAlertSQLTask."""
    return ApplyAlertSQLTaskConfig(
        task_name="test_alerts_task",
        alerts_config="path/to/alerts_config.yaml",
        workspace_path_prefix="path/to/workspace/",
        env_catalog_identifier="catalog_name",
        schema_prefix="schema_prefix",
        df_output_namespace="output_namespace",
        df_output_key="output_key",
    )


@patch("builtins.open", new_callable=mock_open, read_data="alerts:\n  - name: alert1\n  - name: alert2")
def test_get_alerts(mock_file: MagicMock, task: ApplyAlertSQLTask, input_conf: ApplyAlertSQLTaskConfig) -> None:
    """
    Test the get_alerts method of ApplyAlertSQLTask.

    Args:
        mock_file (MagicMock): Mocked file object.
        task (ApplyAlertSQLTask): Instance of ApplyAlertSQLTask.
        input_conf (ApplyAlertSQLTaskConfig): Configuration for ApplyAlertSQLTask.
    """
    alerts = task.get_alerts(input_conf)
    assert len(alerts) == 2
    assert alerts[0]["name"] == "alert1"


def test_dataframe_to_list_of_dicts(spark: SparkSession, logger: logging.Logger, task: ApplyAlertSQLTask) -> None:
    """
    Test the dataframe_to_list_of_dicts method of ApplyAlertSQLTask.

    Args:
        spark (SparkSession): Spark session.
        logger (logging.Logger): Logger instance.
        task (ApplyAlertSQLTask): Instance of ApplyAlertSQLTask.
    """
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
    result = task.dataframe_to_list_of_dicts(df, 2)
    assert len(result) == 2
    assert result[0]["id"] == 1


def test_prepare_card_message(spark: SparkSession, logger: logging.Logger, task: ApplyAlertSQLTask) -> None:
    """
    Test the prepare_card_message method of ApplyAlertSQLTask.

    Args:
        spark (SparkSession): Spark session.
        logger (logging.Logger): Logger instance.
        task (ApplyAlertSQLTask): Instance of ApplyAlertSQLTask.
    """
    alert_config = {"name": "alert1", "table": "test_table", "description": "Test alert"}
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    sample_data = [{"id": 1, "value": "a"}]
    card_message = task.prepare_card_message(alert_config, df, sample_data, 1)
    assert "activityTitle" in card_message
    assert "facts" in card_message
    assert card_message["activityTitle"] == "Alert 1: alert1_test_table"
