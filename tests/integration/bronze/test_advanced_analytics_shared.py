import logging
import random
import string
from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from data_platform.common import build_config
from data_platform.task_runner import TaskRunner

# Set up a logger
logger = logging.getLogger("Testing_bronze_advancing_analytics_shared")

# Secure random generator for unique prefixes
secure_random = random.SystemRandom()


# Function to generate random prefixes
def id_generator(size: int = 6, chars: str = string.ascii_uppercase + string.digits) -> str:
    """Generate a random prefix for schema names."""
    return "".join(secure_random.choice(chars) for _ in range(size))


# Fixture to create and drop temporary schemas in staging and bronze
@pytest.fixture
def configure_bronze_advancing_analytics_shared(spark: SparkSession) -> Generator[str, None, None]:
    prefix = id_generator()
    staging_schema = f"dna_dev_staging.{prefix}test_staging_advancing_analytics_shared"
    bronze_schema = f"dna_dev_bronze.{prefix}test_bronze_advancing_analytics_shared"

    # Create schemas directly in SQL
    logger.info("Creating staging schema: %s", staging_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {staging_schema}")

    logger.info("Creating bronze schema: %s", bronze_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")

    yield prefix

    # Drop schemas after tests
    logger.info("Dropping schema: %s", staging_schema)
    spark.sql(f"DROP SCHEMA IF EXISTS {staging_schema} CASCADE")

    logger.info("Dropping schema: %s", bronze_schema)
    spark.sql(f"DROP SCHEMA IF EXISTS {bronze_schema} CASCADE")


# Helper function to create configuration for TaskRunner
def get_task_config(prefix: str, root_dir: str, table_name: str, id_col: str, source_filter_column: str) -> dict:
    template_path = (
        f"{root_dir}/resources/source/advanced_analytics/templates/"
        "stg2brz_advanced_analytics_foreign_catalog_merge_template.yaml"
    )
    return build_config(
        config_path=template_path,
        parameters={
            "schema_prefix": f"{prefix}",
            "source_table_name": table_name,
            "source_schema_name": "test_staging_advancing_analytics_shared",
            "source_catalog_name": "staging",
            "target_table_name": table_name,
            "target_schema_name": "test_bronze_advancing_analytics_shared",
            "target_catalog_name": "bronze",
            "env_catalog_identifier": "dna_dev_",
            "primary_keys": id_col,
            "source_filter_column": source_filter_column,
            "audit_column_sql": f"{root_dir}/src/data_platform/sql/append_audit_columns_snake_case.sql",
        },
    )


# Load mock data directly into the staging schema table
def load_staging_mock_data(spark: SparkSession, staging_schema: str, table_name: str) -> int:
    data = [
        ("2024-10-24 10:00:00", "2024-10-24 10:05:00", "2024-10-24 10:10:00", "run_1", "series_1", 55.5),
        ("2024-10-25 11:00:00", "2024-10-25 11:05:00", "2024-10-25 11:10:00", "run_2", "series_2", 65.0),
    ]
    columns = [
        "timestamp__UTC__von",
        "time_file_creation__UTC__von",
        "datetime_update_utc",
        "run_time_id",
        "timeseries_id__ch__actual____too",
        "value__ch__actual____ckw",
    ]

    df = spark.createDataFrame(data, columns)
    df = (
        df.withColumn("timestamp__UTC__von", df["timestamp__UTC__von"].cast("timestamp"))
        .withColumn("time_file_creation__UTC__von", df["time_file_creation__UTC__von"].cast("timestamp"))
        .withColumn("datetime_update_utc", df["datetime_update_utc"].cast("timestamp"))
    )
    logger.info(f"Loading mock data into {staging_schema}.{table_name}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{staging_schema}.{table_name}")

    return len(data)


@pytest.mark.parametrize(
    ("table_name", "id_col", "source_filter_column"),
    [
        ("too_timeseries_latest", "run_time_id", "timestamp__UTC__von"),
    ],
)
def test_data_pipeline(
    spark: SparkSession,
    configure_bronze_advancing_analytics_shared: str,
    table_name: str,
    id_col: str,
    root_dir: str,
    source_filter_column: str,
) -> None:
    prefix = configure_bronze_advancing_analytics_shared
    staging_schema = f"dna_dev_staging.{prefix}test_staging_advancing_analytics_shared"
    bronze_schema = f"dna_dev_bronze.{prefix}test_bronze_advancing_analytics_shared"

    logger.info(f"Creating table {table_name} in schema {staging_schema}")
    spark.sql(f"""
        CREATE TABLE {staging_schema}.{table_name} (
            timestamp__UTC__von TIMESTAMP,
            time_file_creation__UTC__von TIMESTAMP,
            datetime_update_utc TIMESTAMP,
            run_time_id STRING,
            timeseries_id__ch__actual____too STRING,
            value__ch__actual____ckw DOUBLE
        ) USING DELTA
    """)

    logger.info(f"Table structure for {staging_schema}.{table_name}:")
    spark.sql(f"DESCRIBE {staging_schema}.{table_name}").show()

    expected_row_count = load_staging_mock_data(spark, staging_schema, table_name)

    task_config = get_task_config(prefix, root_dir, table_name, id_col, source_filter_column)
    task_runner = TaskRunner(spark=spark, init_conf=task_config, logger=logger)
    logger.info(f"Launching TaskRunner for {table_name} transfer from {staging_schema} to {bronze_schema}")
    task_runner.launch()

    bronze_count = spark.sql(f"SELECT COUNT(*) FROM {bronze_schema}.{table_name}").collect()[0][0]
    assert bronze_count == expected_row_count, (
        f"Expected {expected_row_count} rows, found {bronze_count} in bronze table"
    )
