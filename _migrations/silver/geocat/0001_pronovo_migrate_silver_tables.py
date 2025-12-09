# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.geocat import production_plant_table
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


CreateTable(context=context, table_model=production_plant_table).execute()

load_query = f"""INSERT INTO {catalog_prefix}silver.{schema_prefix}geocat.production_plant
SELECT
    xtf_id,
    address,
    post_code,
    _x,
    _y,
    municipality,
    canton,
    main_category,
    sub_category,
    plant_category,
    beginning_of_operation,
    initial_power,
    total_power,
    license,
    'pronovo' AS data_source,
    'geocat' AS data_system,
    valid_from,
    valid_to,
    is_current,
    created_at,
    created_by,
    updated_at,
    updated_by,
    table_id
FROM {catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.production_plant_pronovo"""

spark.sql(load_query)
