import json
import logging
import os
import shutil
from collections.abc import Generator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from data_platform.etl.extract.http.elia import EliaApiReaderExportTask
from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.reader.api.http.ebx import EBXBearerApiReaderTask
from data_platform.tasks.reader.api.http.meteomatics import MeteomaticsApiReaderTask
from data_platform.tasks.reader.api.http.orca import (
    OrcaBearerApiReaderTask,
)
from data_platform.tasks.reader.api.http.request_reader import RequestsApiReaderTask
from data_platform.tasks.reader.api.http.tennet import TennetMeritOrderIterateApiTask
from data_platform.tasks.reader.file import SimpleBatchFileReaderTask, SimpleStreamingFileReaderTask
from data_platform.tasks.reader.sql import SimpleBatchWatermarkReaderTask, SimpleSQLTableReaderTask
from data_platform.tasks.reader.table import (
    SimpleBatchReaderTask,
    SimpleStreamingTableReaderTask,
    SimpleTableReaderTask,
)


@pytest.fixture
def configure_test(spark: SparkSession, logger: logging.Logger, create_schema: str) -> Generator[str, None, None]:
    table_name = "test_reader_table"
    logger.info(f"Create TABLE {create_schema}.{table_name}")
    (spark.range(10).write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.{table_name}"))
    yield f"{create_schema}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")
    logger.info(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")


@pytest.fixture
def configure_test_path_csv(spark: SparkSession) -> Generator[str, None, None]:
    path_name = "/tmp/test/test_table_csv"
    spark.range(10).write.format("csv").mode("overwrite").save(path_name)
    yield path_name
    if os.path.exists(path_name):
        shutil.rmtree(path_name)


@pytest.fixture
def configure_file_test(spark: SparkSession, logger: logging.Logger, create_schema: str) -> Generator[str, None, None]:
    table_name = "test_reader_table"
    logger.info(f"Create TABLE {create_schema}.{table_name}")
    path = f"/tmp/{table_name}"
    (spark.range(10).write.format("delta").mode("overwrite").save(path))
    yield path
    spark.sql(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")
    logger.info(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")


@pytest.fixture
def configure_file_test_csv(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = "test_reader_table_csv"
    logger.info(f"Create TABLE {create_schema}.{table_name}")
    path = f"/tmp/{table_name}"
    (spark.range(10).write.format("csv").mode("overwrite").save(path))
    yield path
    spark.sql(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")
    logger.info(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")


@pytest.fixture
def configure_test_watermark(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    """Configure test table with timestamp data for watermark testing.

    Args:
        spark: SparkSession fixture
        logger: Logger fixture

    Yields:
        str: Name of the test table
    """
    table_name = "test_watermark_batch"
    logger.info(f"Create TABLE {create_schema}.{table_name}")

    # Create sample target table with timestamps
    df = spark.createDataFrame(
        [
            (1, datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")),
            (2, datetime.strptime("2024-01-02 00:00:00", "%Y-%m-%d %H:%M:%S")),
            (3, datetime.strptime("2024-01-03 00:00:00", "%Y-%m-%d %H:%M:%S")),
            (4, datetime.strptime("2024-01-04 00:00:00", "%Y-%m-%d %H:%M:%S")),
            (5, datetime.strptime("2024-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")),
        ],
        schema="id INTEGER, updated_at TIMESTAMP",
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.{table_name}")

    yield f"{create_schema}.{table_name}"

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")
    logger.info(f"DROP TABLE IF EXISTS {create_schema}.{table_name}")


def test_simple_batch_reader_task(spark: SparkSession, configure_test: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple batch reader task")
    table_name = configure_test.split(".")[1]
    schema_name = configure_test.split(".")[0]
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchReaderTask",
                "table_name": table_name,
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test",
                "df_namespace": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleBatchReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    assert task_context.get_property("test", "test").count() == 10


def test_simple_batch_file_reader_task(spark: SparkSession, configure_file_test: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple batch file reader task")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchFileReaderTask",
                "path": configure_file_test,
                "_format": "delta",
                "df_key": "test",
                "df_namespace": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleBatchFileReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    logger.info(task_context.get_property("test", "test").show())

    assert task_context.get_property("test", "test").count() == 10


def test_api_reader_task(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing reading form an API")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "ApiReaderTask",
                "df_key": "api_reader_data",
                "df_namespace": "api_reader",
                "api_url": "https://pypi.org/pypi/sampleproject/json",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = RequestsApiReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    content = task_context.get_property("api_reader", "api_reader_data")

    logger.info(content)

    assert len(content) > 0


@pytest.mark.parametrize("schema", [None, "id string"])
def test_simple_batch_file_reader_task_csv(
    spark: SparkSession, configure_file_test_csv: str, schema: str, logger: logging.Logger
) -> None:
    logger.info("Testing the Simple batch file reader task")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchFileReaderTask",
                "path": configure_file_test_csv,
                "schema": schema,
                "_format": "csv",
                "df_key": "test",
                "df_namespace": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleBatchFileReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    logger.info(task_context.get_property("test", "test").show())

    assert task_context.get_property("test", "test").count() == 10


def test_simple_streaming_table_reader_task(spark: SparkSession, configure_test: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple batch reader task")
    table_name = configure_test.split(".")[1]
    schema_name = configure_test.split(".")[0]
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchReaderTask",
                "table_name": table_name,
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test",
                "df_namespace": "test",
                "reader_options": {"maxFilesPerTrigger": 1},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleStreamingTableReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    df = task_context.get_property("test", "test")

    # View stream in real-time
    query = df.writeStream.format("memory").queryName("test").outputMode("update").trigger(once=True).start()
    query.awaitTermination()
    assert spark.sql("select * from test").count() == 10


def test_simple_sql_table_reader_task(spark: SparkSession, configure_test: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple SQL Table reader task")
    table_name = configure_test.split(".")[1]
    schema_name = configure_test.split(".")[0]
    # Make sure the target table doesn't exist
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.test_sample_read")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleSQLTableReaderTask",
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test_read",
                "df_namespace": "test_read",
                "table_name": table_name,
                "filter_options": {
                    "target_table": f"{schema_name}.test_sample_read",
                    "table_exists": f"select max(id) from {schema_name}.test_sample_read",
                    "table_not_exists": "select 8",
                    "filter_clause": "id > ",
                },
            }
        }
    )
    df = spark.range(5)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{schema_name}.test_sample_read")

    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleSQLTableReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    df = task_context.get_property("test_read", "test_read")

    assert df.count() == 5


def test_simple_table_reader_task(spark: SparkSession, configure_test: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple Table reader task")
    table_name = configure_test.split(".")[1]
    schema_name = configure_test.split(".")[0]
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleTableReaderTask",
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test_read",
                "df_namespace": "test_read",
                "table_name": table_name,
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleTableReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    df = task_context.get_property("test_read", "test_read")

    assert df.count() == 10


def test_simple_streaming_file_reader_task_schema(
    spark: SparkSession, configure_test_path_csv: str, logger: logging.Logger
) -> None:
    logger.info("Testing the Simple batch reader task")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchReaderTask",
                "schema_name": "default",
                "_format": "csv",
                "schema": "id INT",
                "df_key": "test",
                "df_namespace": "test",
                "reader_options": {"maxFilesPerTrigger": 1, "path": configure_test_path_csv},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleStreamingFileReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    df = task_context.get_property("test", "test")

    # View stream in real-time
    query = df.writeStream.format("memory").queryName("test").outputMode("update").trigger(once=True).start()
    query.awaitTermination()
    assert spark.sql("select * from test").count() == 10


@pytest.fixture
def mock_get() -> Generator[Mock, None, None]:
    with patch("requests.get") as mock_get:
        yield mock_get


@pytest.fixture
def mock_post() -> Generator[Mock, None, None]:
    with patch("requests.post") as mock_post:
        yield mock_post


@pytest.fixture
def bearer_api_get_config() -> dict:
    # raw get conf
    return {
        "task_name": "ApiReaderTaskTest",
        "df_namespace": "test_namespace",
        "df_key": "test_key",
        "api_url": "https://api.example.com/data",
        "secret_namespace": "test",
        "access_token_key": "test",
        "api_name": "",
        "api_method": "GET",
        "api_parameters": {"param1": "value1"},
    }


@pytest.fixture
def elia_api_get_config() -> dict:
    return {
        "task_name": "EliaApiReaderExportTaskTest",
        "api_url": "https://api.example.com",
        "api_method": "GET",
        "api_parameters": {
            "dataset_id": "test_dataset",
            "column_to_filter": "date",
            "delta_load_window_days": 7,
            "select_clause": "date, value",
        },
        "volume_storage_path": "/tmp",
        "file_format": "csv",
    }


@pytest.fixture
def orca_bearer_api_get_config() -> dict:
    # raw get conf
    return {
        "task_name": "ApiReaderTaskTest",
        "df_namespace": "test_namespace",
        "df_key": "test_key",
        "api_url": "https://api.example.com/data?From=2025-10-19&To=2025-10-20",
        "secret_namespace": "test",
        "access_token_key": "test",
        "api_name": "",
        "api_method": "GET",
        "api_parameters": {"param1": "value1"},
        "table_name": "test_table",
        "buffer_days": 7,
        "target_snapshot_column": "snapshot_date",
        "default_date": "2024-01-01",
        "current_date": "2024-02-01",
        "bronze_catalog": "bronze_catalog",
        "bronze_schema": "bronze_schema",
        "staging_catalog": "staging_catalog",
        "staging_schema": "staging_schema",
        "volume_name": "default_volume",
        "file_name": "orca_api_data",
    }


@pytest.fixture
def bearer_api_post_config() -> dict:
    # raw post conf
    return {
        "task_name": "ApiReaderTaskTest",
        "df_namespace": "test_namespace",
        "df_key": "test_key",
        "api_url": "https://api.example.com/data",
        "secret_namespace": "test",
        "access_token_key": "test",
        "api_name": "",
        "api_method": "POST",
        "api_parameters": {"param1": "value1"},
    }


def test_execute_valid_meteomatics_api(
    mock_get: Mock, spark: SparkSession, logger: logging.Logger, bearer_api_get_config: dict
) -> None:
    # Mock the API response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "test"
    mock_response.content = str.encode("SomeContentToReturn")
    mock_get.return_value = mock_response

    # Initialize the task
    etl_job = MeteomaticsApiReaderTask()

    # Build api dict into Configuration object
    config = Configuration(bearer_api_get_config)

    # Set up TaskContext
    task_context = TaskContext(logger=logger, spark=spark, configuration=config)

    task_context.put_property(
        bearer_api_get_config["secret_namespace"], bearer_api_get_config["access_token_key"], value="test"
    )

    etl_job.execute(context=task_context, conf=config)

    # Verify Requests invoked appropriately
    mock_get.assert_called_with(
        url="https://api.example.com/data",
        params=None,
        headers={"Content-Type": "application/json", "Authorization": "Bearer test"},
        timeout=1800,
        verify=False,
    )

    # Verify DataFrame creation
    _api_response = task_context.get_property("test_namespace", "test_key")
    logger.info(f"Result of API test call: {_api_response}")
    assert isinstance(_api_response, bytes)
    assert _api_response == str.encode("SomeContentToReturn")


def test_execute_valid_ebx_bearer_api_get_call(
    mock_get: Mock, spark: SparkSession, logger: logging.Logger, bearer_api_get_config: dict
) -> None:
    # Mock the API response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "test"
    mock_response.json.return_value = {"data": "test_data"}
    mock_get.return_value = mock_response

    # Initialize the task
    etl_job = EBXBearerApiReaderTask()

    # Build api dict into Configuration object
    config = Configuration(bearer_api_get_config)

    # Set up TaskContext
    task_context = TaskContext(logger=logger, spark=spark, configuration=config)

    task_context.put_property(
        bearer_api_get_config["secret_namespace"], bearer_api_get_config["access_token_key"], value="test"
    )

    etl_job.execute(context=task_context, conf=config)

    # Verify Requests invoked appropriately
    mock_get.assert_called_with(
        url="https://api.example.com/data",
        params=None,
        headers={"Content-Type": "application/json", "Authorization": "EBX test"},
        timeout=None,
        verify=False,
    )

    # Verify DataFrame creation
    _api_response = task_context.get_property("test_namespace", "test_key")
    logger.info(f"Result of API test call: {_api_response}")
    assert isinstance(_api_response, str | dict)
    assert _api_response == {"data": "test_data"}


def test_execute_valid_elia_api_get_call(
    mock_get: Mock, spark: SparkSession, logger: logging.Logger, elia_api_get_config: dict
) -> None:
    # Mock the API response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"test_csv_content"
    mock_get.return_value = mock_response

    # Initialize the task
    etl_job = EliaApiReaderExportTask()

    # Build api dict into Configuration object
    config = Configuration(elia_api_get_config)

    # Set up TaskContext
    task_context = TaskContext(logger=logger, spark=spark, configuration=config)

    etl_job.execute(context=task_context, conf=config)

    expected_date = (
        datetime.now() - timedelta(days=elia_api_get_config["api_parameters"]["delta_load_window_days"])
    ).strftime("%Y-%m-%d")

    # Verify Requests invoked appropriately
    mock_get.assert_called_with(
        url="https://api.example.com/test_dataset/exports/csv",
        params={"where": f"date>'{expected_date}'", "select": "date, value"},
        headers={},
        timeout=None,
        verify=True,
    )

    # Verify file creation
    logger.info("Test completed successfully.")


def test_execute_valid_orca_bearer_api_get_call(
    mock_get: Mock,
    spark: SparkSession,
    logger: logging.Logger,
    orca_bearer_api_get_config: dict,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify OrcaBearerApiReaderTask executes and writes JSON file to volume.

    This test avoids writing to the real /Volumes path by monkeypatching the task's
    save_to_volume method to write into pytest's tmp_path. It also mocks the HTTP
    GET response so the task proceeds without network access.

    Args:
        mock_get: patched requests.get mock fixture.
        spark: SparkSession fixture.
        logger: test logger fixture.
        orca_bearer_api_get_config: config dict fixture for the Orca task.
        tmp_path: pytest temporary directory fixture.
        monkeypatch: pytest monkeypatch fixture.
    """
    # Arrange - mock HTTP response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "test"
    mock_response.json.return_value = {"data": "test_data"}
    mock_get.return_value = mock_response

    # Initialize task and configuration
    etl_job = OrcaBearerApiReaderTask()
    config = Configuration(orca_bearer_api_get_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=config)

    # Put secret token into context so auth header builds
    task_context.put_property(
        orca_bearer_api_get_config["secret_namespace"],
        orca_bearer_api_get_config["access_token_key"],
        value="test",
    )

    # Monkeypatch save_to_volume to write into tmp_path
    def _mock_save_to_volume(
        self: OrcaBearerApiReaderTask, context: TaskContext, conf: Any, content: Any, from_date: str
    ) -> None:
        """Write content to tmp_path instead of /Volumes for test verification."""

        out_dir = Path(tmp_path) / f"{conf.staging_catalog}_{conf.staging_schema}_{conf.volume_name}"
        out_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        out_file = out_dir / f"{conf.file_name}_{from_date}_{timestamp}.json"
        out_file.write_text(json.dumps(content))
        context.logger.info(f"Mock wrote JSON to {out_file}")

    monkeypatch.setattr(OrcaBearerApiReaderTask, "save_to_volume", _mock_save_to_volume, raising=True)

    # Act - execute the task (synchronous entrypoint)
    etl_job.execute(context=task_context, conf=config)

    # Assert - at least one file was written to tmp_path matching the configured file_name
    search_root = Path(tmp_path)
    written_files = list(search_root.rglob(f"{orca_bearer_api_get_config['file_name']}*.json"))
    assert written_files, f"No output JSON files found under {tmp_path}"

    # Optionally verify the content of one written file matches mocked JSON
    sample = json.loads(written_files[0].read_text())
    assert sample == {"data": "test_data"}

    # Verify that requests.get was invoked at least once
    assert mock_get.called


def test_simple_sql_table_reader_task_with_filters_no_target(
    spark: SparkSession, configure_test: str, logger: logging.Logger
) -> None:
    """Test SimpleSQLTableReaderTask with filter options but no target table."""
    logger.info("Testing the Simple SQL Table reader task with no target table")
    table_name = configure_test.split(".")[1]
    schema_name = configure_test.split(".")[0]

    # Make sure the target table doesn't exist
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.nonexistent_table")

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleSQLTableReaderTask",
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test_read_no_target",
                "df_namespace": "test_read_no_target",
                "table_name": table_name,
                "filter_options": {
                    "target_table": f"{schema_name}.nonexistent_table",
                    "table_exists": f"select max(id) from {schema_name}.nonexistent_table",
                    "table_not_exists": "select 5",
                    "filter_clause": "id > ",
                },
            }
        }
    )

    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleSQLTableReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    df = task_context.get_property("test_read_no_target", "test_read_no_target")
    print(df.show())

    # Should only get records with id > 5 (from table_not_exists query)
    assert df.count() == 4  # Records with id > 5 (6-9)
    assert df.filter("id <= 5").count() == 0  # No records with id <= 5


def test_simple_sql_table_reader_task_without_filters(
    spark: SparkSession, configure_test: str, logger: logging.Logger
) -> None:
    """Test SimpleSQLTableReaderTask without any filter options."""
    logger.info("Testing the Simple SQL Table reader task without filters")
    table_name = configure_test.split(".")[1]
    schema_name = configure_test.split(".")[0]

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleSQLTableReaderTask",
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test_read_no_filter",
                "df_namespace": "test_read_no_filter",
                "table_name": table_name,
            }
        }
    )

    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleSQLTableReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    df = task_context.get_property("test_read_no_filter", "test_read_no_filter")

    # Should get all records since there's no filtering
    assert df.count() == 10  # All records (0-9)


def test_simple_batch_watermark_reader_task_with_watermark(
    spark: SparkSession, configure_test_watermark: str, logger: logging.Logger
) -> None:
    """Test SimpleBatchWatermarkReaderTask with watermark days filter."""
    logger.info("Testing batch reader with watermark days")
    table_name = configure_test_watermark.split(".")[1]
    schema_name = configure_test_watermark.split(".")[0]

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchWatermarkReaderTask",
                "table_name": table_name,
                "schema_name": schema_name,
                "_format": "delta",
                "df_key": "test_batch",
                "df_namespace": "test_batch",
                "watermark_days": 2,
                "watermark_filter_col": "updated_at",
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = SimpleBatchWatermarkReaderTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    result_df = task_context.get_property("test_batch", "test_batch")
    logger.info(result_df.show())
    # Verify filtering
    # With watermark_days=2, we should get records from (max_date - 2 days) onwards
    # Max date is 2024-01-05, so cutoff should be 2024-01-03
    total_count = result_df.count()
    assert total_count == 3  # Should include records from 01-03, 01-04, and 01-05

    # Clean up
    spark.sql(f"DROP TABLE IF EXISTS {schema_name}.test_watermark_batch")


def test_tennet_merit_order_iterate_start_date_no_files(spark: SparkSession, logger: logging.Logger) -> None:
    task_context = TaskContext(logger=logger, spark=spark, configuration=None)
    current_dir_path = os.getcwd().replace("/tests", "")
    start_date = "01-04-1986"
    api_params = {"start_date": start_date, "delta_load_window_days": "5"}
    empty_dir = f"{current_dir_path}/tests/unit/files/tennet/empty"
    if not os.path.exists(empty_dir):
        os.makedirs(empty_dir)
    dt = TennetMeritOrderIterateApiTask._calculate_start_date(task_context, empty_dir, api_params)
    assert dt.strftime("%d-%m-%Y") == start_date


def test_tennet_merit_order_iterate_start_date_delta_no_checkpoint(spark: SparkSession, logger: logging.Logger) -> None:
    task_context = TaskContext(logger=logger, spark=spark, configuration=None)
    current_dir_path = os.getcwd().replace("/tests", "")
    start_date = "01-04-1986"
    api_params = {"start_date": start_date, "delta_load_window_days": "5"}
    test_dir = f"{current_dir_path}/tests/unit/files/tennet/delta_nocheckpoint"
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)
    open(f"{test_dir}/1752278400_meritorderrecords_12-07-2025_20250714090826.csv", "w").close()
    open(f"{test_dir}/1752364800_meritorderrecords_13-07-2025_20250714090828.csv", "w").close()
    open(f"{test_dir}/1752451200_meritorderrecords_14-07-2025_20250714090830", "w").close()
    dt = TennetMeritOrderIterateApiTask._calculate_start_date(task_context, test_dir, api_params)
    assert dt.strftime("%d-%m-%Y") == "10-07-2025"


def test_tennet_merit_order_iterate_start_date_delta_checkpoint(spark: SparkSession, logger: logging.Logger) -> None:
    task_context = TaskContext(logger=logger, spark=spark, configuration=None)
    current_dir_path = os.getcwd().replace("/tests", "")
    start_date = "01-04-1986"
    api_params = {"start_date": start_date, "delta_load_window_days": "5"}
    test_dir = f"{current_dir_path}/tests/unit/files/tennet/delta_checkpoint"
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)
    if not os.path.exists(f"{test_dir}/checkpoint"):
        os.makedirs(f"{test_dir}/checkpoint")
    open(f"{test_dir}/1752278400_meritorderrecords_12-07-2025_20250714090826.csv", "w").close()
    open(f"{test_dir}/1752364800_meritorderrecords_13-07-2025_20250714090828.csv", "w").close()
    open(f"{test_dir}/1752451200_meritorderrecords_14-07-2025_20250714090830", "w").close()
    dt = TennetMeritOrderIterateApiTask._calculate_start_date(task_context, test_dir, api_params)
    assert dt.strftime("%d-%m-%Y") == "10-07-2025"
