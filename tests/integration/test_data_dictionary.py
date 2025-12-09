import logging
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.common import build_config
from data_platform.task_runner import TaskRunner

# Set up a logger
logger = logging.getLogger("Testing_data_dictionary")


@pytest.fixture
def transactional_data_dataframe(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("curve_name", StringType(), True),
            StructField("value_at", TimestampType(), True),
            StructField("value", DoubleType(), True),
            StructField("curve_type", StringType(), True),
        ]
    )
    data = [
        (
            "pxx xxx xxx mwh/h xxx min15 x",
            datetime.fromisoformat("2023-05-01T00:00:00.000+02:00"),
            11.00000,
            "time series",
        ),
        (
            "pxx xxx xxx mwh/h xxx min15 x",
            datetime.fromisoformat("2023-05-01T00:00:00.000+02:00"),
            2.68000,
            "time series",
        ),
        (
            "pxx xxx xxx mwh/h xxx min15 x",
            datetime.fromisoformat("2023-05-01T00:00:00.000+02:00"),
            3.12000,
            "time series",
        ),
        (
            "pxx xxx xxx mwh/h xxx min15 x",
            datetime.fromisoformat("2023-05-01T00:00:00.000+02:00"),
            25.36000,
            "time series",
        ),
    ]
    df = spark.createDataFrame(data, schema)
    return df


@pytest.fixture
def integration_test_config(
    spark: SparkSession,
    transactional_data_dataframe: DataFrame,
    create_schema: Any,
    create_volume: Any,
    create_table_from_dataframe: Any,
    copy_file_to_uc_volume: Any,
    id_generator: str,
    root_dir: str,
    logger: logging.Logger,
) -> Generator:
    """Fixture to combine schema, volume, table, and file setup into a single reusable configuration."""

    # Variable setup
    env_catalog_identifier = ""
    catalog_name = "dna_dev_bronze"
    schema_prefix = id_generator.lower()
    schema_name = "_integration_test_schema"
    volume_name = "data_dictionary_volume"
    table_name = "data_dictionary"
    transactional_table_name = "solar_production"
    source_file_name = "data_dictionary.csv"
    source_file_config_path = f"tests/integration/files/csv/{source_file_name}"
    source_path = f"{root_dir}/{source_file_config_path}"  # Combine root_dir and file path
    uc_volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

    # Log source path for debugging
    logger.info(f"Source path resolved to: {source_path}")

    # Ensure file exists before proceeding
    if not Path(source_path).exists():
        raise FileNotFoundError(f"Source file '{source_path}' does not exist.")

    try:
        # Step 1: Create the schema
        full_schema_name = create_schema(catalog_name, f"{schema_prefix}{schema_name}")
        logger.info(f"Schema created: {full_schema_name}")

        # Step 2: Create the volume
        full_volume_name = create_volume(catalog_name, f"{schema_prefix}{schema_name}", volume_name)
        logger.info(f"Volume created: {full_volume_name}")

        # Step 3: Copy file to Unity Catalog volume
        copy_file_to_uc_volume(source_file_config_path, catalog_name, f"{schema_prefix}{schema_name}", volume_name)
        logger.info(f"File copied to volume: {uc_volume_path}")

        # Step 4: Create the table in the schema
        create_table_from_dataframe(
            dataframe=transactional_data_dataframe,
            catalog_name=catalog_name,
            schema_name=f"{schema_prefix}{schema_name}",
            table_name=transactional_table_name,
        )
        logger.info(f"Table created: {catalog_name}.{schema_prefix}{schema_name}")

        # Return all configuration variables for use in tests
        config_vars = {
            "env_catalog_identifier": env_catalog_identifier,
            "schema_prefix": schema_prefix,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "volume_name": volume_name,
            "table_name": table_name,
            "uc_volume_path": uc_volume_path,
            "source_path": source_path,
            "root_path": root_dir,
            "transactional_table_name": transactional_table_name,
        }
        yield config_vars

    finally:
        # Teardown logic
        logger.info("Cleaning up resources.")
        create_schema.cleanup(catalog_name, f"{schema_prefix}{schema_name}")


def build_task_config(config_vars: dict[str, Any], id_col: str) -> dict:
    """Builds a task configuration with substituted environment variables."""

    # Resolved paths and parameters
    env_catalog_identifier = config_vars["env_catalog_identifier"]
    catalog_name = config_vars["catalog_name"]
    schema_prefix = config_vars["schema_prefix"]
    schema_name = config_vars["schema_name"]
    table_name = config_vars["table_name"]
    volume_name = config_vars["volume_name"]
    sql_file_path = f"{config_vars['root_path']}/tests/integration/files/sql/data_dictionary_merge.sql"
    audit_column_sql = f"{config_vars['root_path']}/sql/append_audit_columns_snake_case.sql"

    return build_config(
        config_path=f"{config_vars['root_path']}/conf/data_dictionary.yml",
        parameters={
            "env_catalog_identifier": env_catalog_identifier,
            "catalog_name": catalog_name,
            "schema_prefix": schema_prefix,
            "schema_name": schema_name,
            "table_name": table_name,
            "volume_name": volume_name,
            "source_file_format": "csv",
            "table_schema": "catalog_name STRING, schema_name STRING, table_name STRING, column_name STRING, \
column_comment STRING, proposed_tag_key STRING, proposed_tag_value STRING, created_at TIMESTAMP, created_by STRING, \
updated_at TIMESTAMP, updated_by STRING",
            "dataframe_schema": "catalog_name STRING, schema_name STRING, table_name STRING, column_name STRING, \
column_comment STRING, proposed_tag_key STRING, proposed_tag_value STRING",
            "header": "True",
            "maxFilesPerTrigger": "1",
            "forceDeleteTempCheckpointLocation": "True",
            "sql_file_path": sql_file_path,
            "audit_column_sql": audit_column_sql,
            "writer_options": {
                "checkpoint": "1",
                "forceDeleteTempCheckpointLocation": "True",
            },
            "id": id_col,
        },
    )


@pytest.mark.skip(reason="JVM erroring out on shared cluster")
def test_data_pipeline(spark: SparkSession, integration_test_config: dict[str, Any]) -> None:
    id_col = "Data Dictionary Run 1"
    logger = logging.getLogger("test_data_pipeline")
    logger.info("test_setup = %s", integration_test_config)
    print("test_setup = ", integration_test_config)
    # Use test_setup dictionary as config_vars in build_task_config
    task_config = build_task_config(config_vars=integration_test_config, id_col=id_col)
    # Initialize and run the TaskRunner
    task_runner = TaskRunner(spark=spark, init_conf=task_config, logger=logger)
    logger.info("Running TaskRunner")
    task_runner.launch()

    # Run assertions on Unity Catalog tags
    verify_column_tags(spark, integration_test_config)


def verify_column_tags(spark: SparkSession, config_vars: dict[str, Any]) -> None:
    env_catalog_identifier = config_vars["env_catalog_identifier"]
    catalog_name = config_vars["catalog_name"]
    schema_prefix = config_vars["schema_prefix"]
    schema_name = config_vars["schema_name"]
    table_name = config_vars["table_name"]
    transactional_table_name = config_vars["transactional_table_name"]

    # Query Unity Catalog column tags
    tags_sql = f"""
        SELECT DISTINCT column_name, tag_name, tag_value
        FROM system_tables_sharing.system_tables_information_schema.column_tags
        WHERE catalog_name = '{env_catalog_identifier}{catalog_name}'
          AND schema_name = '{schema_prefix}{schema_name}'
          AND table_name = '{transactional_table_name}'
    """
    tags_df = spark.sql(tags_sql)

    # Query the data dictionary to get expected tags
    data_dictionary_df = spark.table(
        f"{env_catalog_identifier}{catalog_name}.{schema_prefix}{schema_name}.{table_name}"
    )
    expected_tags_df = data_dictionary_df.select("column_name", "proposed_tag_key", "proposed_tag_value").filter(
        (col("proposed_tag_key").isNotNull()) & (col("proposed_tag_value").isNotNull())
    )

    # Compare counts for validation
    assert tags_df.count() == expected_tags_df.count(), (
        f"Expected {expected_tags_df.count()} tags, found {tags_df.count()} in Unity Catalog."
    )

    # Compare the actual and expected tags for each row, UC stores tag keys as lower case?!!!!!
    mismatched_tags = tags_df.join(
        expected_tags_df,
        (tags_df.column_name == expected_tags_df.column_name)
        & (lower(tags_df.tag_name) == lower(expected_tags_df.proposed_tag_key))
        & (tags_df.tag_value == expected_tags_df.proposed_tag_value),
        "leftanti",
    )
    assert mismatched_tags.count() == 0, "Tag mismatch found between Unity Catalog and data dictionary."
    if mismatched_tags.count() > 0:
        mismatched_tags.show(truncate=False)
    logger.info("All tags matched between Unity Catalog and data dictionary.")
