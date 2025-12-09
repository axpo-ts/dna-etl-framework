# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

table_name_target = (
    f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.intraday_price_volume_flex_xbids"
)
table_name_source = f"{catalog_prefix}bronze.{schema_prefix}advanced_analytics_shared.too_timeseries_latest"
primary_keys = "delivery_start,value_column".split(",")
columns_to_add = "run_time_id,project_name_FR_actual_too,time_file_creation_UTC_von".split(",")
filter = "timeseries_id__ch__actual____too LIKE '%settled%' AND timeseries_id__ch__actual____too LIKE '%intraday%'"

if spark.catalog.tableExists(table_name_target):
    df = spark.table(table_name_target)
    if columns_to_add[0] not in df.columns:
        # add the missing columns
        (
            df.join(
                spark.table(table_name_source)
                .where(filter)
                .withColumnsRenamed(
                    {
                        "timestamp__UTC__von": "delivery_start",
                        "timeseries_id__ch__actual____too": "value_column",
                        "project_name__FR__actual____too": "project_name_fr_actual_too",
                        "time_file_creation__UTC__von": "time_file_creation_utc_von",
                    }
                )
                .select(*primary_keys, *columns_to_add),
                primary_keys,
                "left",
            )
        ).createOrReplaceTempView("source_data")

        # rerwite the resulting table
        (
            spark.sql(f"""
            SELECT
                delivery_start,
                delivery_end,
                value_column,
                value, -- noqa: RF04
                updated_at_source,
                run_time_id,
                project_name_fr_actual_too,
                created_at,
                created_by,
                updated_at,
                updated_by
            FROM source_data
            """)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name_target)
        )
