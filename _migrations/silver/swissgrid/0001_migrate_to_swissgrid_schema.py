# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from data_platform.data_model import StaticTableModel
from data_platform.data_model.silver.swissgrid import activated_volume_afrr_energy_table, attribute_table
from data_platform.etl.core.task_context import TaskContext
from data_platform.etl.load.create_table import CreateTable

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821


context = TaskContext(
    dbutils=dbutils,  # noqa: F821
    catalog_prefix=catalog_prefix,
    schema_prefix=schema_prefix,
)


def create_table(table_model: StaticTableModel) -> str:  # noqa: D103
    table_name = table_model.identifier.name
    spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}volue.{table_name}")
    CreateTable(context=context, table_model=table_model).execute()
    return table_name


swissgrid_tables = [activated_volume_afrr_energy_table, attribute_table]

for table in swissgrid_tables:
    create_table(table)

# COMMAND ----------


def migrate_swissgrid_tables(source_schema: str, source_table: str, target_table: str) -> None:  # noqa: D103
    source_table_df = spark.table(f"{catalog_prefix}silver.{source_schema}.{source_table}")
    source_table_df = (
        source_table_df.withColumnRenamed("unit_type", "unit")
        .withColumn("data_source", lit("swissgrid"))
        .withColumn("data_system", lit("swissgrid"))
    )

    source_table_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_prefix}silver.{schema_prefix}swissgrid.{target_table}"
    )


table_mapping = [
    {
        "source_schema": "market_data_and_fundamentals",
        "source_table": "swissgrid_activation_signal_picasso",
        "target_table": "activated_volume_afrr_energy",
    },
    {"source_schema": "attributes", "source_table": "swissgrid", "target_table": "attribute"},
]

for table_map in table_mapping:
    source_schema = table_map["source_schema"]
    source_table = table_map["source_table"]
    target_table = table_map["target_table"]

    migrate_swissgrid_tables(source_schema, source_table, target_table)
