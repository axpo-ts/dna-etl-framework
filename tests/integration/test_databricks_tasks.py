# Test for data_platform tasks which require databricks
import logging
import random
import string
from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.schema.volume.volume import CreateVolumeTask
from data_platform.tasks.writer.volume.volume import JsonToVolumeWriterTask

secure_random = random.SystemRandom()


def id_generator(size: int = 6, chars: str = string.ascii_uppercase + string.digits) -> str:
    return "".join(random.choice(chars) for _ in range(size))


@pytest.fixture
def setup_test_schema(spark: SparkSession) -> Generator:
    random_id = id_generator()
    schema_name = f"{random_id}_test".lower()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS dna_dev_staging.{schema_name}")
    yield schema_name
    spark.sql(f"DROP SCHEMA dna_dev_staging.{schema_name} CASCADE")


def test_write_json_to_volume_with_attributes(
    spark: SparkSession, logger: logging.Logger, setup_test_schema: str
) -> None:
    """Test creating a volume and writing a json object to it, with optional attributes"""
    logger.info("Testing the JsonToVolumeWriterTask")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "CreateVolumeTask",
                "schema_name": setup_test_schema,
                "catalog_name": "dna_dev_staging",
                "volume_name": "test",
                "comment": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = CreateVolumeTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    result = spark.sql(f"DESCRIBE VOLUME dna_dev_staging.{setup_test_schema}.test")
    assert result.count() == 1

    # write json object to volume
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "CreateVolumeTask",
                "schema_name": setup_test_schema,
                "catalog_name": "dna_dev_staging",
                "volume_name": "test",
                "file_name": "test",
                "df_input_namespace": "test",
                "df_input_key": "test",
                "add_timestamp": "True",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test", '{ "name":"John", "age":30, "city":"New York"}')
    etl_job_json = JsonToVolumeWriterTask()
    etl_job_json.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    count_result: int = spark.sql(f"LIST 'dbfs:/Volumes/dna_dev_staging/{setup_test_schema}/test/'").count()
    assert count_result == 1


def test_write_json_to_volume(spark: SparkSession, logger: logging.Logger, setup_test_schema: str) -> None:
    """Test creating a volume and writing a json object to it, with optional attributes."""
    logger.info("Testing the JsonToVolumeWriterTask")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "CreateVolumeTask",
                "schema_name": setup_test_schema,
                "catalog_name": "dna_dev_staging",
                "volume_name": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = CreateVolumeTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
    result = spark.sql(f"DESCRIBE VOLUME dna_dev_staging.{setup_test_schema}.test")
    assert result.count() == 1

    # write json object to volume
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "JsonToVolumeWriterTask",
                "schema_name": setup_test_schema,
                "catalog_name": "dna_dev_staging",
                "volume_name": "test",
                "file_name": "test",
                "df_input_namespace": "test",
                "df_input_key": "test",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("test", "test", '{ "name":"John", "age":30, "city":"New York"}')
    etl_job_json = JsonToVolumeWriterTask()
    etl_job_json.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    count_result: int = spark.sql(f"LIST 'dbfs:/Volumes/dna_dev_staging/{setup_test_schema}/test/'").count()
    assert count_result == 1
