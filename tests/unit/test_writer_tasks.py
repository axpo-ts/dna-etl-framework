import logging
import os
import shutil
import zipfile
from collections.abc import Generator
from io import BytesIO
from unittest import mock

import pytest
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession, types
from pyspark.sql import functions as f

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.schema.table.table import CreateMetadataTableTask, CreateTableTask
from data_platform.tasks.writer.sql import BatchSQLWriterTask, StreamingSQLWriterTask
from data_platform.tasks.writer.table import (
    CloneTableTask,
    SelectiveOverwriteTask,
    SimpleBatchUpsertChangesTask,
    SimpleBatchWriterTask,
    SimpleStreamingUpsertChangesTask,
    SimpleStreamingWriterTask,
    SimpleType2MergeTask,
)
from data_platform.tasks.writer.utils import WriterUtilities
from data_platform.tasks.writer.volume.volume import (
    ZipCSVObjectToVolumeWriterTask,
)


# custom function as pyspark.testing.assertDataFrameEqual is not working with given numpy version
# TODO: check if this is still needed when using PySpark version greater than 3.5.0
def assert_data_frame_equal(df1: DataFrame, df2: DataFrame) -> None:
    assert df1.schema == df2.schema, "Schemas are not equal"
    df1_sorted = df1.sort(*df1.columns)
    df2_sorted = df2.sort(*df2.columns)
    assert df1_sorted.collect() == df2_sorted.collect(), "DataFrames are not equal"


@pytest.fixture
def configure_test_writer(spark: SparkSession, logger: logging.Logger, create_schema: str) -> Generator:
    table_name = f"{create_schema}.test_reader_table"
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE {table_name}")
    tempdir = "/tmp/test"
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)


@pytest.fixture
def configure_test_batch_upsert_changes_target(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_upsert_changes_target"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                ("1", "A", "Alice", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("2", "B", "Bob", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("3", "C", "Charlie", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("4", "D", "Diana", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("5", "E", None, "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("6", "F", None, "2023-11-06", "user_1", "2023-11-06", "user_1"),
            ],
            """label_1 string, label_2 string, value string,
            created_at string, created_by string, updated_at string, updated_by string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_batch_upsert_changes_source(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_upsert_changes_source"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                ("2", "B", "Bob", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("3", "C", "Chris", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("4", "D", None, "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("5", "E", "Eve", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("6", "F", None, "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("7", "G", "George", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("2", "C", "Charles", "2023-11-15", "user_2", "2023-11-15", "user_2"),
            ],
            """label_1 string, label_2 string, value string,
            created_at string, created_by string, updated_at string, updated_by string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_stream_upsert_changes_target(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_upsert_changes_stream_target"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                ("1", "A", "Alice", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("2", "B", "Bob", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("3", "C", "Charlie", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("4", "D", "Diana", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("5", "E", None, "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("6", "F", None, "2023-11-06", "user_1", "2023-11-06", "user_1"),
            ],
            """label_1 string, label_2 string, value string,
            created_at string, created_by string, updated_at string, updated_by string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_stream_upsert_changes_source_with_duplicates(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_upsert_changes_source_with_duplicates"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                ("2", "B", "Bob", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("2", "B", "Bob", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("3", "C", "Chris", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("4", "D", None, "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("5", "E", "Eve", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("6", "F", "Don", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("6", "F", "Don", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("7", "F", "Florin", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("7", "F", "Florin", "2023-11-15", "user_2", "2023-11-15", "user_2"),
            ],
            """label_1 string, label_2 string, value string,
            created_at string, created_by string, updated_at string, updated_by string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_expected_after_upsert_changes(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_expected_after_upsert_changes"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                ("1", "A", "Alice", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("2", "B", "Bob", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("3", "C", "Chris", "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("4", "D", None, "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("5", "E", "Eve", "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("6", "F", None, "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("7", "G", "George", "2023-11-15", "user_2", "2023-11-15", "user_2"),
                ("2", "C", "Charles", "2023-11-15", "user_2", "2023-11-15", "user_2"),
            ],
            """label_1 string, label_2 string, value string,
            created_at string, created_by string, updated_at string, updated_by string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_expected_after_duplicate_upsert_changes(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_expected_after_upsert_changes_duplicates"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                ("1", "A", "Alice", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("2", "B", "Bob", "2023-11-06", "user_1", "2023-11-06", "user_1"),
                ("3", "C", "Chris", "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("4", "D", None, "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("5", "E", "Eve", "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("6", "F", "Don", "2023-11-06", "user_1", "2023-11-15", "user_2"),
                ("7", "F", "Florin", "2023-11-15", "user_2", "2023-11-15", "user_2"),
            ],
            """label_1 string, label_2 string, value string,
            created_at string, created_by string, updated_at string, updated_by string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_type2_merge_changes_target(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_type2_merge_changes_target"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                (101, "Alpha", None, None, True),
                (102, "Bravo", None, None, True),
                (103, "Charlie", None, None, True),
            ],
            """primary_key int, test_value string, valid_from timestamp,
                valid_to timestamp, is_current boolean""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_type2_merge_changes_source(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.test_type2_merge_changes_source"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                (101, "Alpha2"),
                (104, "Delta"),
            ],
            """primary_key int, test_value string""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture
def configure_test_expected_after_type2_merge_changes(
    spark: SparkSession, logger: logging.Logger, create_schema: str
) -> Generator[str, None, None]:
    table_name = f"{create_schema}.configure_test_expected_after_type2_merge_changes"
    logger.info(f"CREATE TABLE {table_name}")
    (
        spark.createDataFrame(
            [
                (101, "Alpha", False),
                (102, "Bravo", True),
                (103, "Charlie", True),
                (104, "Delta", True),
                (101, "Alpha2", True),
            ],
            """primary_key int, test_value string, is_current boolean""",
        )
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    logger.info(f"DROP TABLE IF EXISTS {table_name}")


def test_type2_merge_changes_task(
    spark: SparkSession,
    configure_test_type2_merge_changes_target: str,
    configure_test_type2_merge_changes_source: str,
    configure_test_expected_after_type2_merge_changes: str,
    logger: logging.Logger,
) -> None:
    logger.info("Testing the type2 merge changes task")

    configure_test_type2_merge_changes_target_table_name = configure_test_type2_merge_changes_target.split(".")[1]
    configure_test_type2_merge_changes_target_schema_name = configure_test_type2_merge_changes_target.split(".")[0]

    configure_test_type2_merge_changes_source_table_name = configure_test_type2_merge_changes_source.split(".")[1]
    configure_test_type2_merge_changes_source_schema_name = configure_test_type2_merge_changes_source.split(".")[0]

    configure_test_expected_after_type2_merge_changes_table_name = (
        configure_test_expected_after_type2_merge_changes.split(".")[1]
    )
    configure_test_expected_after_type2_merge_changes_schema_name = (
        configure_test_expected_after_type2_merge_changes.split(".")[0]
    )

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleType2MergeTask",
                "table_name": configure_test_type2_merge_changes_target_table_name,
                "schema_name": configure_test_type2_merge_changes_target_schema_name,
                "df_key": "test",
                "df_namespace": "test",
                "primary_keys": "primary_key",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    source_df = spark.table(
        f"{configure_test_type2_merge_changes_source_schema_name}.{configure_test_type2_merge_changes_source_table_name}"
    )
    task_context.put_property("test", "test", source_df)
    etl_job = SimpleType2MergeTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    merged_df = spark.sql(
        f"select primary_key, test_value, is_current from {configure_test_type2_merge_changes_target_schema_name}.{configure_test_type2_merge_changes_target_table_name}"  # noqa: E501
    )
    expected_df = spark.table(
        f"{configure_test_expected_after_type2_merge_changes_schema_name}.{configure_test_expected_after_type2_merge_changes_table_name}"
    )
    assert_data_frame_equal(merged_df, expected_df)


def test_simple_batch_upsert_changes_task(
    spark: SparkSession,
    configure_test_batch_upsert_changes_target: str,
    configure_test_batch_upsert_changes_source: str,
    configure_test_expected_after_upsert_changes: str,
    logger: logging.Logger,
) -> None:
    logger.info("Testing the simple batch upsert changes task")

    configure_test_batch_upsert_changes_target_table_name = configure_test_batch_upsert_changes_target.split(".")[1]
    configure_test_batch_upsert_changes_target_schema_name = configure_test_batch_upsert_changes_target.split(".")[0]

    configure_test_batch_upsert_changes_source_table_name = configure_test_batch_upsert_changes_source.split(".")[1]
    configure_test_batch_upsert_changes_source_schema_name = configure_test_batch_upsert_changes_source.split(".")[0]

    configure_test_expected_after_upsert_changes_table_name = configure_test_expected_after_upsert_changes.split(".")[1]
    configure_test_expected_after_upsert_changes_schema_name = configure_test_expected_after_upsert_changes.split(".")[
        0
    ]

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchUpsertChangesTask",
                "table_name": configure_test_batch_upsert_changes_target_table_name,
                "schema_name": configure_test_batch_upsert_changes_target_schema_name,
                "df_key": "test",
                "df_namespace": "test",
                "primary_keys": "label_1, label_2",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    source_df = spark.table(
        f"{configure_test_batch_upsert_changes_source_schema_name}.{configure_test_batch_upsert_changes_source_table_name}"
    )
    task_context.put_property("test", "test", source_df)
    etl_job = SimpleBatchUpsertChangesTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    upserted_df = spark.table(
        f"{configure_test_batch_upsert_changes_target_schema_name}.{configure_test_batch_upsert_changes_target_table_name}"
    )
    expected_df = spark.table(
        f"{configure_test_expected_after_upsert_changes_schema_name}.{configure_test_expected_after_upsert_changes_table_name}"
    )
    assert_data_frame_equal(upserted_df, expected_df)


def test_simple_streaming_upsert_changes_task(
    spark: SparkSession,
    configure_test_batch_upsert_changes_target: str,
    configure_test_batch_upsert_changes_source: str,
    configure_test_expected_after_upsert_changes: str,
    logger: logging.Logger,
) -> None:
    logger.info("Testing the simple streaming upsert changes task")

    configure_test_batch_upsert_changes_target_table_name = configure_test_batch_upsert_changes_target.split(".")[1]
    configure_test_batch_upsert_changes_target_schema_name = configure_test_batch_upsert_changes_target.split(".")[0]

    configure_test_batch_upsert_changes_source_table_name = configure_test_batch_upsert_changes_source.split(".")[1]
    configure_test_batch_upsert_changes_source_schema_name = configure_test_batch_upsert_changes_source.split(".")[0]

    configure_test_expected_after_upsert_changes_table_name = configure_test_expected_after_upsert_changes.split(".")[1]
    configure_test_expected_after_upsert_changes_schema_name = configure_test_expected_after_upsert_changes.split(".")[
        0
    ]
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleStreamingUpsertChangesTask",
                "table_name": configure_test_batch_upsert_changes_target_table_name,
                "schema_name": configure_test_batch_upsert_changes_target_schema_name,
                "df_key": "test",
                "df_namespace": "test",
                "primary_keys": "label_1, label_2",
                "trigger_kwargs": {"once": True},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    source_df = spark.readStream.table(
        f"{configure_test_batch_upsert_changes_source_schema_name}.{configure_test_batch_upsert_changes_source_table_name}"
    )
    task_context.put_property("test", "test", source_df)
    etl_job = SimpleStreamingUpsertChangesTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    upserted_df = spark.table(
        f"{configure_test_batch_upsert_changes_target_schema_name}.{configure_test_batch_upsert_changes_target_table_name}"
    )
    expected_df = spark.table(
        f"{configure_test_expected_after_upsert_changes_schema_name}.{configure_test_expected_after_upsert_changes_table_name}"
    )
    assert_data_frame_equal(upserted_df, expected_df)


def test_simple_streaming_upsert_changes_ignore_columns_task(
    spark: SparkSession,
    create_schema: str,
    logger: logging.Logger,
) -> None:
    logger.info("Testing the simple streaming upsert changes ignore columns task")
    spark.createDataFrame(
        [
            ("A", "Alice", "2023-11-06", "user_1", "2023-11-06", "user_1"),
            ("B", "Bob", "2023-11-06", "user_1", "2023-11-06", "user_1"),
            ("C", "Charlie", "2023-11-06", "user_1", "2023-11-06", "user_1"),
        ],
        """category string, value string,
                created_at string, created_by string, updated_at string, updated_by string""",
    ).write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_table_stream_source")

    spark.createDataFrame(
        [
            (1, "A", "Fred", "2023-11-06", "user_1", "2023-11-06", "user_1"),
            (2, "B", "Billy", "2023-11-06", "user_1", "2023-11-06", "user_1"),
        ],
        """id int, category string, value string,
                created_at string, created_by string, updated_at string, updated_by string""",
    ).write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_table_stream_target")

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleStreamingUpsertChangesTask",
                "table_name": "test_table_stream_target",
                "schema_name": create_schema,
                "df_key": "test_input_table",
                "df_namespace": "test",
                "primary_keys": "category",
                "ignore_columns": "id",
                "trigger_kwargs": {"once": True},
            }
        }
    )
    source_df = spark.readStream.table(f"{create_schema}.test_table_stream_source")
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test_input_table", source_df)

    etl_job = SimpleStreamingUpsertChangesTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    upserted_df = spark.table(f"{create_schema}.test_table_stream_target").drop("id")
    expected_df = spark.table(f"{create_schema}.test_table_stream_source")
    assert_data_frame_equal(upserted_df, expected_df)


def test_simple_streaming_upsert_changes_dedupe_on_streaming_task(
    spark: SparkSession,
    configure_test_stream_upsert_changes_target: str,
    configure_test_stream_upsert_changes_source_with_duplicates: str,
    configure_test_expected_after_duplicate_upsert_changes: str,
    logger: logging.Logger,
) -> None:
    logger.info("Testing the simple streaming upsert changes task")

    configure_test_stream_upsert_changes_target_table_name = configure_test_stream_upsert_changes_target.split(".")[1]
    configure_test_stream_upsert_changes_target_schema_name = configure_test_stream_upsert_changes_target.split(".")[0]

    configure_test_stream_upsert_changes_source_with_duplicates_table_name = (
        configure_test_stream_upsert_changes_source_with_duplicates.split(".")[1]
    )
    configure_test_stream_upsert_changes_source_with_duplicates_schema_name = (
        configure_test_stream_upsert_changes_source_with_duplicates.split(".")[0]
    )

    configure_test_expected_after_duplicate_upsert_changes_table_name = (
        configure_test_expected_after_duplicate_upsert_changes.split(".")[1]
    )
    configure_test_expected_after_duplicate_upsert_changes_schema_name = (
        configure_test_expected_after_duplicate_upsert_changes.split(".")[0]
    )

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleStreamingUpsertChangesTask",
                "table_name": configure_test_stream_upsert_changes_target_table_name,
                "schema_name": configure_test_stream_upsert_changes_target_schema_name,
                "df_key": "test",
                "df_namespace": "test",
                "primary_keys": "label_1, label_2",
                "trigger_kwargs": {"once": True},
                "drop_duplicates": True,
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    source_df = spark.readStream.table(
        f"{configure_test_stream_upsert_changes_source_with_duplicates_schema_name}.{configure_test_stream_upsert_changes_source_with_duplicates_table_name}"
    )

    task_context.put_property("test", "test", source_df)
    etl_job = SimpleStreamingUpsertChangesTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    upserted_df = spark.table(
        f"{configure_test_stream_upsert_changes_target_schema_name}.{configure_test_stream_upsert_changes_target_table_name}"
    )
    expected_df = spark.table(
        f"{configure_test_expected_after_duplicate_upsert_changes_schema_name}.{configure_test_expected_after_duplicate_upsert_changes_table_name}"
    )

    assert_data_frame_equal(upserted_df, expected_df)


def test_simple_batch_writer_task(spark: SparkSession, configure_test_writer: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple batch writer task")
    configure_test_writer_table_name = configure_test_writer.split(".")[1]
    configure_test_writer_schema_name = configure_test_writer.split(".")[0]

    logger.info("Create metadata table")
    test_metadata_config = Configuration(
        {"1": {"task_name": "CreateMetadataTableTask", "schema_name": configure_test_writer_schema_name}}
    )
    logger.info(test_metadata_config)
    task_context_metadata = TaskContext(logger=logger, spark=spark, configuration=test_metadata_config)
    metadata_table_creation = CreateMetadataTableTask()
    metadata_table_creation.execute(context=task_context_metadata, conf=test_metadata_config.get_tree("1"))

    logger.info("Write the test table")
    df = spark.range(10)
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchWriterTask",
                "table_name": configure_test_writer_table_name,
                "schema_name": configure_test_writer_schema_name,
                "_format": "delta",
                "output_mode": "overwrite",
                "df_key": "test",
                "df_namespace": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test", df)
    etl_job = SimpleBatchWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert (
        spark.sql(f"select * from {configure_test_writer_schema_name}.{configure_test_writer_table_name}").count() == 10
    )


@pytest.mark.skip(reason="Shared cluster cant use checkpoint")
def test_simple_streaming_writer_task(spark: SparkSession, configure_test_writer: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple streaming writer task")
    configure_test_writer_table_name = configure_test_writer.split(".")[1]
    configure_test_writer_schema_name = configure_test_writer.split(".")[0]
    (
        spark.range(10)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{configure_test_writer_schema_name}.test_table")
    )
    df = spark.readStream.table(f"{configure_test_writer_schema_name}.test_table")

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchWriterTask",
                "table_name": configure_test_writer_table_name,
                "schema_name": configure_test_writer_schema_name,
                "_format": "delta",
                "output_mode": "append",
                "df_key": "test",
                "df_namespace": "test",
                "writer_options": {"checkpointLocation": "dbfs:/tmp/test/"},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test", df)
    etl_job = SimpleStreamingWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert (
        spark.sql(f"select * from {configure_test_writer_schema_name}.{configure_test_writer_table_name}").count() == 10
    )


def test_write_to_memory_streaming_task(
    spark: SparkSession, configure_test_writer: Generator[str, None, None], logger: logging.Logger, create_schema: str
) -> None:
    logger.info("Testing the Write to Memory streaming writer task")

    # Create a test DataFrame
    (spark.range(10).write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_table_stream"))
    df = spark.readStream.table(f"{create_schema}.test_table_stream")
    df.createOrReplaceTempView("test_input_table")

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleStreamingWriterTask",
                "table_name": "test_memory_table",
                "schema_name": create_schema,
                "_format": "memory",
                "output_mode": "append",
                "df_key": "test_input_table",
                "df_namespace": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test_input_table", df)

    # Instantiate and run the task
    etl_job = SimpleStreamingWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    # Check if the data exists in the memory view
    result_df = spark.sql("SELECT * FROM q_test_memory_table")

    # Assert that the result matches the expected data
    assert result_df.count() == 10


def test_selective_overwrite_task(
    spark: SparkSession, configure_test_writer: Generator[str, None, None], logger: logging.Logger, create_schema: str
) -> None:
    logger.info("Testing the Selective Overwrite task")
    (
        spark.range(10)
        .withColumn("SourceSystem", f.lit("Snowflake"))
        .withColumn("Timestamp", f.lit(f.current_timestamp()))
        .withColumn("__partition_by", f.lit(f.current_date()))
        .write.format("delta")
        .partitionBy("__partition_by")
        .mode("overwrite")
        .saveAsTable(f"{create_schema}.test_overwrite_table")
    )
    df = (
        spark.range(5)
        .withColumn("SourceSystem", f.lit("Snowflake"))
        .withColumn("Timestamp", f.lit(f.current_timestamp()))
    )

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SelectiveOverwriteTask",
                "table_name": "test_overwrite_table",
                "schema_name": create_schema,
                "_format": "delta",
                "output_mode": "overwrite",
                "df_key": "test_overwrite",
                "df_namespace": "test_overwrite",
                "writer_options": {"partition_cols": "__partition_by", "partition_expression": "to_date(Timestamp)"},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test_overwrite", "test_overwrite", df)
    etl_job = SelectiveOverwriteTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert spark.sql(f"select * from {create_schema}.test_overwrite_table").count() == 5


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_streaming_sql_writer_task(spark: SparkSession, logger: logging.Logger, create_schema: str) -> None:
    # logging.info("Testing the Simple streaming writer task")

    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_upsert_table")

    # create new batch df
    df = spark.createDataFrame([(0, "z"), (2, "c")], schema=schema)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_table")

    df2 = spark.readStream.table(f"{create_schema}.test_table")
    # upsert query
    query = f"""
    MERGE INTO {create_schema}.test_upsert_table t
    USING test s
    ON s.id = t.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "StreamingSQLWriterTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_alias": "test",
                "trigger_kwargs": {"once": True},
                "sql": query,
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put new batch df in context
    task_context.put_property("input_namespace", "input_df", df2)

    etl_job = StreamingSQLWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql(f"select * from {create_schema}.test_upsert_table")

    # output table should have 3 rows
    assert output_table.count() == 3
    # should have id: 0 -> value: z (update)
    assert output_table.filter("id == 0").select("value").collect()[0][0] == "z"
    # should have id: 2 -> value: c (insert)
    assert output_table.filter("id == 2").select("value").collect()[0][0] == "c"


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_batch_sql_writer_task(spark: SparkSession, logger: logging.Logger) -> None:
    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable("default.test_upsert_table")

    # create new batch df
    df = spark.createDataFrame([(0, "z"), (2, "c")], schema=schema)

    # upsert query
    query = """
    MERGE INTO default.test_upsert_table t
    USING test s
    ON s.id = t.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "BatchSQLWriterTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_alias": "test",
                "sql": query,
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put new batch df in context
    task_context.put_property("input_namespace", "input_df", df)

    etl_job = BatchSQLWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql("select * from default.test_upsert_table")

    # output table should have 3 rows
    assert output_table.count() == 3
    # should have id: 0 -> value: z (update)
    assert output_table.filter("id == 0").select("value").collect()[0][0] == "z"
    # should have id: 2 -> value: c (insert)
    assert output_table.filter("id == 2").select("value").collect()[0][0] == "c"


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_streaming_sql_file_writer_task(spark: SparkSession, logger: logging.Logger) -> None:
    # logging.info("Testing the Simple streaming writer task")

    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable("default.test_upsert_table")

    # create new batch df
    df = spark.createDataFrame([(0, "z"), (2, "c")], schema=schema)
    df.write.format("delta").mode("overwrite").saveAsTable("default.test_stream_table")

    df2 = spark.readStream.table("default.test_stream_table")

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "StreamingSQLWriterTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_alias": "test",
                "trigger_kwargs": {"once": True},
                "sql_file": "tests/unit/files/test_stream_sql.sql",
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put new batch df in context
    task_context.put_property("input_namespace", "input_df", df2)

    etl_job = StreamingSQLWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql("select * from default.test_upsert_table")

    # output table should have 3 rows
    assert output_table.count() == 3
    # should have id: 0 -> value: z (update)
    assert output_table.filter("id == 0").select("value").collect()[0][0] == "z"
    # should have id: 2 -> value: c (insert)
    assert output_table.filter("id == 2").select("value").collect()[0][0] == "c"


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_batch_sql_file_writer_task(spark: SparkSession, logger: logging.Logger) -> None:
    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable("default.test_upsert_table")

    # create new batch df
    df = spark.createDataFrame([(0, "z"), (2, "c")], schema=schema)

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "BatchSQLWriterTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_alias": "test",
                "sql_file": "tests/unit/files/test_stream_sql.sql",
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put new batch df in context
    task_context.put_property("input_namespace", "input_df", df)

    etl_job = BatchSQLWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql("select * from default.test_upsert_table")

    # output table should have 3 rows
    assert output_table.count() == 3
    # should have id: 0 -> value: z (update)
    assert output_table.filter("id == 0").select("value").collect()[0][0] == "z"
    # should have id: 2 -> value: c (insert)
    assert output_table.filter("id == 2").select("value").collect()[0][0] == "c"


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_streaming_sql_file_vars_writer_task(spark: SparkSession, logger: logging.Logger) -> None:
    # logging.info("Testing the Simple streaming writer task")

    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable("default.test_upsert_table")

    # create new batch df
    df = spark.createDataFrame([(0, "z"), (2, "c")], schema=schema)
    df.write.format("delta").mode("overwrite").saveAsTable("default.test_stream_vars_table")

    df2 = spark.readStream.table("default.test_stream_vars_table")

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "StreamingSQLWriterTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_alias": "test",
                "trigger_kwargs": {"once": True},
                "sql_file": "tests/unit/files/test_stream_sql.sql",
                "sql_file_vars": {"table_name": "test_upsert_table"},
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put new batch df in context
    task_context.put_property("input_namespace", "input_df", df2)

    etl_job = StreamingSQLWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql("select * from default.test_upsert_table")

    # output table should have 3 rows
    assert output_table.count() == 3
    # should have id: 0 -> value: z (update)
    assert output_table.filter("id == 0").select("value").collect()[0][0] == "z"
    # should have id: 2 -> value: c (insert)
    assert output_table.filter("id == 2").select("value").collect()[0][0] == "c"


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_batch_sql_file_vars_writer_task(spark: SparkSession, logger: logging.Logger) -> None:
    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable("default.test_upsert_table")

    # create new batch df
    df = spark.createDataFrame([(0, "z"), (2, "c")], schema=schema)

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "BatchSQLWriterTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_alias": "test",
                "sql_file": "tests/unit/files/test_stream_sql.sql",
                "sql_file_vars": {"table_name": "test_upsert_table"},
            }
        }
    )

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put new batch df in context
    task_context.put_property("input_namespace", "input_df", df)

    etl_job = BatchSQLWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql("select * from default.test_upsert_table")

    # output table should have 3 rows
    assert output_table.count() == 3
    # should have id: 0 -> value: z (update)
    assert output_table.filter("id == 0").select("value").collect()[0][0] == "z"
    # should have id: 2 -> value: c (insert)
    assert output_table.filter("id == 2").select("value").collect()[0][0] == "c"


def test_clone_table_writer_task(spark: SparkSession, logger: logging.Logger, create_schema: str) -> None:
    # setup initial table
    schema = types.StructType(
        [types.StructField("id", types.IntegerType()), types.StructField("value", types.StringType())]
    )
    initial_df = spark.createDataFrame([(0, "a"), (1, "b")], schema=schema)
    initial_df.write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_upsert_table")

    # etl config
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "CloneTableTask",
                "source_schema_name": create_schema,
                "source_table_name": "test_upsert_table",
                "target_schema_name": create_schema,
                "target_table_name": "test_upsert_table3",
                "clone_type": "SHALLOW",
            }
        }
    )
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    etl_job = CloneTableTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    output_table = spark.sql(f"select * from {create_schema}.test_upsert_table3")
    assert output_table.count() == 2


def test_clone_table_writer_task_mock(spark: SparkSession, logger: logging.Logger, create_schema: str) -> None:
    with mock.patch.object(spark, "sql") as mock_sql:
        expected_query = f"""CREATE OR REPLACE TABLE `{create_schema}`.`test_upsert_table3` DEEP CLONE `{create_schema}`.`test_upsert_table` TBLPROPERTIES(delta.appendOnly=True, delta.dataSkippingNumIndexedCols='2') LOCATION somewhere/over/therainbow"""  # noqa: E501

        test_etl_config = Configuration(
            {
                "1": {
                    "task_name": "CloneTableTask",
                    "source_schema_name": create_schema,
                    "source_table_name": "test_upsert_table",
                    "target_schema_name": create_schema,
                    "target_table_name": "test_upsert_table3",
                    "clone_type": "DEEP",
                    "table_properties": {"delta.appendOnly": True, "delta.dataSkippingNumIndexedCols": "2"},
                    "location": "somewhere/over/therainbow",
                }
            }
        )
        task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

        etl_job = CloneTableTask()
        etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
        mock_sql.assert_called_once_with(expected_query)


@pytest.mark.skip(reason="id not matching on shared cluster")
def test_create_table_method(spark: SparkSession, configure_test_writer: str, logger: logging.Logger) -> None:
    logger.info("Testing the Simple batch writer task")
    configure_test_writer_table_name = configure_test_writer.split(".")[1]
    configure_test_writer_schema_name = configure_test_writer.split(".")[0]
    spark.sql(f"DROP TABLE IF EXISTS {configure_test_writer}")
    logger.info("Create metadata table")
    test_metadata_config = Configuration(
        {
            "1": {
                "task_name": "CreateTableTask",
                "schema_name": configure_test_writer_schema_name,
                "table_name": configure_test_writer_table_name,
                "table_schema": "id int",
            }
        }
    )
    logger.info(test_metadata_config)
    task_context_metadata = TaskContext(logger=logger, spark=spark, configuration=test_metadata_config)
    metadata_table_creation = CreateTableTask()
    metadata_table_creation.execute(context=task_context_metadata, conf=test_metadata_config.get_tree("1"))

    logger.info("Write the test table")
    df = spark.range(10)
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "SimpleBatchWriterTask",
                "table_name": configure_test_writer_table_name,
                "schema_name": configure_test_writer_schema_name,
                "_format": "delta",
                "output_mode": "append",
                "df_key": "test",
                "df_namespace": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test", df)
    etl_job = SimpleBatchWriterTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert (
        spark.sql(f"select * from {configure_test_writer_schema_name}.{configure_test_writer_table_name}").count() == 10
    )


def test_zip_csv_to_volume_writer_task(spark: SparkSession, logger: logging.Logger) -> None:
    """Test the ZipCSVObjectToVolumeWriterTask."""
    # Create test CSV content and zip it
    csv_content = "col1,col2\n1,a\n2,b"
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr("test.csv", csv_content)
    zip_content = zip_buffer.getvalue()

    # Configure task
    test_config = Configuration(
        {
            "1": {
                "task_name": "ZipCSVObjectToVolumeWriterTask",
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "volume_name": "test_volume",
                "file_name": "test_file",
                "file_format": "csv",
                "df_input_namespace": "test",
                "df_input_key": "test_zip",
                "add_timestamp": False,
                "data_content_type": "string",
            }
        }
    )

    # Set up context with test data
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_config)
    task_context.put_property("test", "test_zip", zip_content)

    # Mock file writing function
    with mock.patch("builtins.open", mock.mock_open()) as mock_file:
        writer_task = ZipCSVObjectToVolumeWriterTask()
        writer_task.execute(context=task_context, conf=test_config.get_tree("1"))

        # Verify file write operations
        expected_path = "/Volumes/test_catalog/test_schema/test_volume/test.csv"
        mock_file.assert_called_once_with(expected_path, "wb")
        mock_file().write.assert_called_once_with(csv_content.encode())


def test_build_update_column_list(
    spark: SparkSession, configure_test_batch_upsert_changes_source: str, logger: logging.Logger
) -> None:
    logger.info("Testing list of columns to update after processing exclusion list")
    configure_test_batch_upsert_changes_source_table_name = configure_test_batch_upsert_changes_source.split(".")[1]
    configure_test_batch_upsert_changes_source_schema_name = configure_test_batch_upsert_changes_source.split(".")[0]
    spark.table(
        f"{configure_test_batch_upsert_changes_source_schema_name}.{configure_test_batch_upsert_changes_source_table_name}"
    )
    primary_keys = ["label_1", "label_2"]
    remaining_columns = ["value", "created_at", "created_by", "updated_at", "updated_by"]
    updateable_columns = WriterUtilities.build_update_column_list(
        DeltaTable.forName(
            spark,
            f"{configure_test_batch_upsert_changes_source_schema_name}.{configure_test_batch_upsert_changes_source_table_name}",
        ),
        primary_keys,
    )
    logger.info(updateable_columns)
    assert updateable_columns == remaining_columns
