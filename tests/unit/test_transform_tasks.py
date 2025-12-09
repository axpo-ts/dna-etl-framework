import logging
import os
from io import BytesIO

import pandas as pd
import xarray as xr
from pyspark.sql import SparkSession

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.transform.apply_pyspark import ApplyPysparkTask
from data_platform.tasks.transform.apply_sql import ApplySQLTask
from data_platform.tasks.transform.dataframe import (
    IncrementalTablesReaderTask,
    JoinDataFrameTask,
    UnionByNameDataFrameTask,
)
from data_platform.tasks.transform.netcdf import NetCdfToCsvTask


def test_apply_sql_file_task(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the Apply SQL task")
    df = spark.range(10)
    current_dir = os.getcwd().replace("/tests", "")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "ApplySQLTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_output_namespace": "output_namespace",
                "df_output_key": "output_df",
                "df_alias": "test",
                "sql_file": f"{current_dir}/tests/unit/files/test_sql.sql",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task_context.put_property("input_namespace", "input_df", df)
    etl_job = ApplySQLTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert task_context.get_property("output_namespace", "output_df").count() == 10
    assert task_context.get_property("output_namespace", "output_df").columns == ["_id", "test_col"]


def test_incremental_tables_reader_task(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing IncrementalTablesReaderTask")

    # Case 1: Target table does not exist
    base_df = spark.createDataFrame(
        [(1, "A", "2025-01-01 00:00:00", "user1"), (2, "B", "2025-01-02 00:00:00", "user2")],
        ["id", "value", "updated_at", "created_by"],
    )

    additional_df = spark.createDataFrame(
        [(3, "C", "2025-01-03 00:00:00", "user3")], ["id", "value", "updated_at", "created_by"]
    )

    second_additional_df = spark.createDataFrame(
        [(4, "D", "2025-01-04 00:00:00", "user4")], ["id", "value", "updated_at", "created_by"]
    )

    # Register temporary tables
    base_df.createOrReplaceTempView("time_series_a")
    additional_df.createOrReplaceTempView("time_series_s")
    second_additional_df.createOrReplaceTempView("time_series_n")

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "IncrementalTablesReaderTask",
                "source_tables": {
                    "base": "time_series_a",
                    "additional": "time_series_s",
                    "second_additional": "time_series_n",
                    "third_additional": "",  # empty table should be skipped
                    "fourth_additional": None,  # None table should be skipped
                },
                "target_table": "target_table_name",  # Target table does not exist
                "watermark_column": "updated_at",
                "df_output_namespace": "output_ns",
                "df_output_key": "unioned_df",
                "drop_duplicates": True,
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task = IncrementalTablesReaderTask()
    task.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    # Retrieve the output
    df_out = task_context.get_property("output_ns", "unioned_df")

    # There should be 4 rows total (from base + additional + second_additional)
    assert df_out.count() == 4

    # Columns that remain should include id and value
    assert "id" in df_out.columns
    assert "value" in df_out.columns

    logger.info("IncrementalTablesReaderTask test passed successfully for case without target table")

    # Case 2: Target table exists
    base_df.createOrReplaceTempView("time_series_a")
    additional_df.createOrReplaceTempView("time_series_s")
    second_additional_df.createOrReplaceTempView("time_series_n")

    # Create a target table with a row having value as 2025-01-01 12:00:00
    target_df = spark.createDataFrame([(1, "Y", "2025-01-01 12:00:00")], ["id", "value", "updated_at"])
    target_df.createOrReplaceTempView("target_table_name")

    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task = IncrementalTablesReaderTask()
    task.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    # Retrieve the output
    df_out = task_context.get_property("output_ns", "unioned_df")

    # There should  be 3 rows total (from base (Incremental) + additional + second_additional)
    assert df_out.count() == 3

    # Columns that remain should include id and value
    assert "id" in df_out.columns
    assert "value" in df_out.columns

    logger.info("IncrementalTablesReaderTask test passed successfully for case with target table")


def test_apply_sql_file_task_with_vars(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the Apply SQL task with sql_file_vars")
    df = spark.range(10)
    current_dir = os.getcwd().replace("/tests", "")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "ApplySQLTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_output_namespace": "output_namespace",
                "df_output_key": "output_df",
                "df_alias": "test",
                "sql_file": f"{current_dir}/tests/unit/files/test_sql_vars.sql",
                "sql_file_vars": {"$table": "test", "$col": "mycol"},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task_context.put_property("input_namespace", "input_df", df)
    etl_job = ApplySQLTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert task_context.get_property("output_namespace", "output_df").count() == 10
    assert task_context.get_property("output_namespace", "output_df").columns == ["_id", "mycol"]


def test_transformer(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the ApplyPysparkTask with parameters")
    df = spark.range(10)
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "ApplyPysparkTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_output_namespace": "output_namespace",
                "df_output_key": "output_df",
                "transformations": [{"func": "select_cols", "args": {"select_statement": "id"}}],
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task_context.put_property("input_namespace", "input_df", df)
    etl_job = ApplyPysparkTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert task_context.get_property("output_namespace", "output_df").count() == 10


def test_join_dataframe(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the JoinDataFrameTask with parameters")
    df_left = spark.range(10)
    df_right = spark.range(10)
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "JoinDataFrameTask",
                "df_input_namespace_left": "input_namespace_left",
                "df_input_key_left": "input_df_left",
                "df_input_namespace_right": "input_namespace_right",
                "df_input_key_right": "input_df_right",
                "df_output_namespace": "output_namespace",
                "df_output_key": "output_df",
                "join_on": ["id"],
                "join_how": "inner",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task_context.put_property("input_namespace_left", "input_df_left", df_left)
    task_context.put_property("input_namespace_right", "input_df_right", df_right)
    etl_job = JoinDataFrameTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert task_context.get_property("output_namespace", "output_df").count() == 10


def test_union_by_name_dataframe(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the UnionByNameDataFrameTask")
    df_1 = spark.range(10).withColumnRenamed("id", "id_1")
    df_2 = spark.range(5).withColumnRenamed("id", "id_2")
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "JoinDataFrameTask",
                "df_input_namespace_first": "input_namespace_1",
                "df_input_key_first": "input_df_1",
                "df_input_namespace_second": "input_namespace_2",
                "df_input_key_second": "input_df_2",
                "df_output_namespace": "output_namespace",
                "df_output_key": "output_df",
                "allow_missing_columns": "True",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    task_context.put_property("input_namespace_1", "input_df_1", df_1)
    task_context.put_property("input_namespace_2", "input_df_2", df_2)
    etl_job = UnionByNameDataFrameTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert task_context.get_property("output_namespace", "output_df").count() == df_1.count() + df_2.count()
    assert task_context.get_property("output_namespace", "output_df").columns == ["id_1", "id_2"]


def test_execute_netcdf_to_csv_task_with_csv_input(spark: SparkSession, logger: logging.Logger) -> None:
    """Task should pass through CSV string unchanged when api_output_format = csv."""
    conf_dict = {
        "task_name": "NetCdfToCsvTask",
        "df_input_namespace": "ns",
        "df_input_key": "in_key",
        "df_output_namespace": "ns_out",
        "df_output_key": "out_key",
        "api_output_format": "csv",
    }
    test_etl_config = Configuration(conf_dict)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    # put CSV string in context
    csv_string = "a,b\n1,2\n3,4\n"
    task_context.put_property("ns", "in_key", csv_string)

    task = NetCdfToCsvTask()
    task.execute(task_context, test_etl_config)

    result = task_context.get_property("ns_out", "out_key")
    assert result == csv_string


def test_execute_with_netcdf_input(spark: SparkSession, logger: logging.Logger) -> None:
    """Task should convert NetCDF bytes to CSV string."""
    conf_dict = {
        "task_name": "NetCdfToCsvTask",
        "df_input_namespace": "ns",
        "df_input_key": "in_key",
        "df_output_namespace": "ns_out",
        "df_output_key": "out_key",
        "api_output_format": "netcdf",
        "columns_to_drop": None,
    }
    test_etl_config = Configuration(conf_dict)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)

    # Create a simple xarray dataset and serialize to NetCDF bytes
    ds = xr.Dataset({"temp": ("x", [10.0, 12.0, 14.0])}, coords={"x": [0, 1, 2]})
    buffer = BytesIO()
    ds.to_netcdf(buffer, engine="h5netcdf")
    buffer.seek(0)

    task_context.put_property("ns", "in_key", buffer.getvalue())

    task = NetCdfToCsvTask()
    task.execute(task_context, test_etl_config)

    result = task_context.get_property("ns_out", "out_key")
    # Should be a CSV string with columns including 'x' and 'temp'
    df = pd.read_csv(BytesIO(result.encode("utf-8")))
    assert list(df.columns) == ["x", "temp"]
    assert df.shape[0] == 3
