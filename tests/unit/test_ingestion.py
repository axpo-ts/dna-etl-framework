import logging
import os
from datetime import datetime

import pandas as pd  # type: ignore
from pyspark.sql import SparkSession

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.ingestion.ingestion import GetTimeBlocksTask, SetDeltaLoadParametersBulkTask


def test_get_time_blocks_task_overwrite(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the 'GetTimeBlocksTask' task")
    test_cases = {
        "1": {  # simple case, parameters given as string and using append operation
            "task_name": "GetTimeBlocksTask",
            "outputs": {"namespace": "test", "key": "time_blocks"},
            "start_timestamp": "2024-10-01T10:00:00",
            "end_timestamp": "2024-10-05T22:00:00",
            "block_width_days": "3",
            "time_format": "%Y-%m-%dT%H:%M:%S",
        },
        "2": {  # a 2nd case, parameters given as string and using overwrite operation
            "task_name": "GetTimeBlocksTask",
            "outputs": {"namespace": "test", "key": "time_blocks"},
            "start_timestamp": "2024-10-01T10:00:00",
            "end_timestamp": "2024-10-05T22:00:00",
            "block_width_days": "3",
            "write_mode": "overwrite",
        },
        "3": {  # a 3rd case, parameters read from task context
            "task_name": "GetTimeBlocksTask",
            "outputs": {"namespace": "test", "key": "time_blocks"},
            "inputs": {
                "namespace": "test",
                "start_timestamp": "start",
                "end_timestamp": "end",
                "block_width_days": "width",
            },
            "write_mode": "overwrite",
        },
    }
    expected_results = {
        "1": [
            {
                "block_num": 0,
                "start_time": "2024-10-01T10:00:00",
                "end_time": "2024-10-04T10:00:00",
                "delta_days": 3.0,
                "write_mode": "append",
            },
            {
                "block_num": 1,
                "start_time": "2024-10-04T10:00:00",
                "end_time": "2024-10-05T22:00:00",
                "delta_days": 1.5,
                "write_mode": "append",
            },
        ],
        "2": [
            {
                "block_num": 0,
                "start_time": "2024-10-01T10:00:00",
                "end_time": "2024-10-04T10:00:00",
                "delta_days": 3.0,
                "write_mode": "overwrite",
            },
            {
                "block_num": 1,
                "start_time": "2024-10-04T10:00:00",
                "end_time": "2024-10-05T22:00:00",
                "delta_days": 1.5,
                "write_mode": "append",
            },
        ],
        "3": [
            {
                "block_num": 0,
                "start_time": "2024-10-01T00:00:00",
                "end_time": "2024-10-03T00:00:00",
                "delta_days": 2.0,
                "write_mode": "overwrite",
            }
        ],
    }

    test_config = Configuration(test_cases)
    logger.info(test_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_config)

    # add task context variables
    task_context.put_property("test", "start", datetime(2024, 10, 1))
    task_context.put_property("test", "end", datetime(2024, 10, 3))
    task_context.put_property("test", "width", 10)

    etl_job = GetTimeBlocksTask()
    for test_index in ["1", "2", "3"]:
        etl_job.execute(context=task_context, conf=test_config.get_tree(test_index))
        result = task_context.get_property("test", "time_blocks")
        logger.info(result)
        assert result == expected_results[test_index]


def test_set_delta_load_parameters_task(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the 'SetDeltaLoadParametersBulkTask' task")

    current_dir_path = os.getcwd().replace("/tests", "")
    config_path = f"{current_dir_path}/tests/unit/files/test_mds_api_curves.json"

    # Create a small bronze-like mock table for testing
    bronze_data = [
        ("49633001", datetime(2025, 3, 19, 0, 0, 0)),
        ("1000001389", datetime(2025, 3, 19, 0, 0, 0)),
    ]
    bronze_df = spark.createDataFrame(bronze_data, ["Identifier", "ReferenceTime"])
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    bronze_df.write.format("delta").mode("overwrite").saveAsTable("test_schema.test_table")

    test_cases = {
        "1": {
            "task_name": "SetDeltaLoadParametersBulkTask",
            "inputs": {
                "full_load": "True",
                "from_date": "",
                "to_date": "",
                "resource_ids": "",
                "full_load_window_days": "365",
                "delta_load_window_days": "30",
                "config_path": config_path,
                "schema": "test_schema",
            },
            "outputs": {"namespace": "test", "key": "test1"},
        },
        "2": {
            "task_name": "SetDeltaLoadParametersBulkTask",
            "inputs": {
                "full_load": "False",
                "from_date": "",
                "to_date": "",
                "resource_ids": "",
                "full_load_window_days": "365",
                "delta_load_window_days": "30",
                "config_path": config_path,
                "schema": "test_schema",
            },
            "outputs": {"namespace": "test", "key": "test2"},
        },
        "3": {
            "task_name": "SetDeltaLoadParametersBulkTask",
            "inputs": {
                "full_load": "True",
                "from_date": "2024-01-01",
                "to_date": "2024-12-31",
                "resource_ids": "1000001389,48052751",
                "config_path": config_path,
                "full_load_window_days": "365",
                "delta_load_window_days": "30",
                "schema": "test_schema",
            },
            "outputs": {"namespace": "test", "key": "test3"},
        },
        "4": {
            "task_name": "SetDeltaLoadParametersBulkTask",
            "inputs": {
                "full_load": "True",
                "from_date": "",
                "to_date": "2025-10-31",
                "resource_ids": "1000001389,48052751",
                "config_path": config_path,
                "full_load_window_days": "365",
                "delta_load_window_days": "30",
                "schema": "test_schema",
            },
            "outputs": {"namespace": "test", "key": "test4"},
        },
    }

    expected_results = {
        "1": {"start_time": "2024-01-01", "end_time": datetime.today().strftime("%Y-%m-%d")},
        "2": {
            "start_time": (datetime.today() - pd.Timedelta(days=30)).strftime("%Y-%m-%d"),
            "end_time": datetime.today().strftime("%Y-%m-%d"),
        },
        "3": {"start_time": "2024-01-01", "end_time": "2024-12-31"},
        "4": {"start_time": "2024-01-01", "end_time": "2025-10-31"},
    }

    test_config = Configuration(test_cases)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_config)

    etl_job = SetDeltaLoadParametersBulkTask()
    for test_index in ["1", "2", "3", "4"]:
        etl_job.execute(context=task_context, conf=test_config.get_tree(test_index))
        result = task_context.get_property("test", f"test{test_index}")

        # Flatten to find global min start and max end
        min_start_time = min(result, key=lambda x: x["start_time"])["start_time"][:10]
        max_end_time = max(result, key=lambda x: x["end_time"])["end_time"][:10]
        print(min_start_time, max_end_time)

        assert {
            "start_time": min_start_time,
            "end_time": max_end_time,
        } == expected_results[test_index]
