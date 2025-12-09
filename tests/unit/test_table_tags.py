import logging

from pyspark.sql import SparkSession

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.schema.table.table import CreateTableTagsTask


def test_create_table_tags_task(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the Create Metadata Table task")

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "CreateTableTagsTask",
                "schema_name": "test_schema",
                "catalog_name": "test_catalog",
                "table_name": "test_table",
                "tags": {"test": "test", "test2": "test2"},
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    etl_job = CreateTableTagsTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
