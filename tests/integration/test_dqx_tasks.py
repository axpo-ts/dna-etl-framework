import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.dq.dqx.apply_dqx import ApplyDqxRulesTask


def test_apply_dqx_rules_task(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing the Apply Dqx Rules Task")

    schema = StructType([StructField("primary_key", IntegerType(), True), StructField("name", StringType(), True)])

    input_df = spark.createDataFrame(
        [
            (1, "A"),
            (2, "B"),
        ],
        schema,
    )

    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "ApplyDqxRulesTask",
                "df_input_namespace": "input_namespace",
                "df_input_key": "input_df",
                "df_output_namespace": "output_namespace",
                "df_output_key": "output_df",
                "target_schema_name": "output_schema",
                "target_table_name": "output_table",
                "dq_rules_file_path": "tests/integration/files/json/test_dqx_rules.json",
                "dq_rules_table": "output_dq_table",
                "dq_metrics_table": "output_metrics_table",
                "dq_quarantine_table": "output_quarantine_table",
            }
        }
    )
    logger.info(test_etl_config)
    task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
    task_context.put_property("input_namespace", "input_df", input_df)
    etl_job = ApplyDqxRulesTask()
    etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

    assert task_context.get_property("output_namespace", "output_df").count() == 2
