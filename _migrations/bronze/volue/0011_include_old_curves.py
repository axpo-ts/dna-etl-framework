# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, IntegerType, StringType, TimestampType

spark = SparkSession.builder.getOrCreate()

"""
Ad-hoc migration script to include the old curves in the `curves_attributes` table.
"""

# COMMAND ----------

# Configuration variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore

# COMMAND ----------
# Define the patterns to filter the required curves
# Patterns to identify the curves related to precipitation and temperature, respectively
p_pattern = "%weather30 gwh cet h sa"
t_pattern = "%con °c weather40 cet h sa"

# Read the source table and filter the curves based on the defined patterns
df = (
    spark.table(f"{catalog_prefix}bronze.{schema_prefix}volue.time_series_sa")
    .filter((F.col("curve_name").like(p_pattern)) | (F.col("curve_name").like(t_pattern)))
    .select("curve_name")
    .distinct()
)

# COMMAND ----------
# Add the required columns with appropriate values for the attributes table
df_result = (
    # As we don't have the original IDs, we create negative IDs to highlight they are manually created
    df.withColumn("id", -1000 - F.row_number().over(Window.orderBy("curve_name")))
    .withColumn("name", F.col("curve_name"))
    .withColumn("frequency", F.lit("H"))
    .withColumn("time_zone", F.lit("CET"))
    .withColumn("curve_type", F.lit("TIME_SERIES"))
    .withColumn("curve_state", F.lit("PUBLIC"))
    .withColumn("created", F.current_timestamp())
    .withColumn("modified", F.current_timestamp())
    .withColumn("area", F.upper(F.split("curve_name", " ")[1]))
    .withColumn(
        "categories",
        F.when(F.col("curve_name").like(p_pattern), F.array(F.lit("RRE")))
        .when(F.col("curve_name").like(t_pattern), F.array(F.lit("TT"), F.lit("CON")))
        .otherwise(F.lit(None)),
    )
    .withColumn("commodity", F.lit("POW"))
    .withColumn(
        "unit",
        F.when(F.col("curve_name").like(p_pattern), F.lit("GWH"))
        .when(F.col("curve_name").like(t_pattern), F.lit("°C"))
        .otherwise(F.lit(None)),
    )
    .withColumn("has_access", F.lit("false"))
    .withColumn("access_range_begin", F.lit(None).cast("timestamp"))
    .withColumn("access_range_end", F.lit(None).cast("timestamp"))
    .withColumn("data_type", F.lit("SA"))
    .withColumn(
        "description",
        F.when(
            F.col("curve_name").like(p_pattern),
            F.lit(
                "This curve is related to precipitation and it is no longer available in the API. "
                "Its ids and attributes were created manually."
            ),
        )
        .when(
            F.col("curve_name").like(t_pattern),
            F.lit(
                "This curve is related to temperature and it is no longer available in the API. "
                "Its ids and attributes were created manually."
            ),
        )
        .otherwise(F.lit(None)),
    )
    .withColumn("license", F.lit("WATTSIGH_INFSERVICE_WATTSIGH"))
    .withColumn("created_at", F.expr("current_timestamp() - interval 1 day"))
    .withColumn("created_by", F.lit("ad-hoc-migration-script-0011"))
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("updated_by", F.lit("ad-hoc-migration-script-0011"))
)

# Select and cast columns to the correct types according to the schema of the target table
df_with_schema = df_result.select(
    F.col("id").cast(IntegerType()),
    F.col("name").cast(StringType()),
    F.col("frequency").cast(StringType()),
    F.col("time_zone").cast(StringType()),
    F.col("curve_type").cast(StringType()),
    F.col("curve_state").cast(StringType()),
    F.col("created").cast(TimestampType()),
    F.col("modified").cast(TimestampType()),
    F.col("area").cast(StringType()),
    F.col("categories").cast(ArrayType(StringType())),
    F.col("commodity").cast(StringType()),
    F.col("unit").cast(StringType()),
    F.col("has_access").cast(StringType()),
    F.col("access_range_begin").cast(TimestampType()),
    F.col("access_range_end").cast(TimestampType()),
    F.col("data_type").cast(StringType()),
    F.col("description").cast(StringType()),
    F.col("license").cast(StringType()),
    F.col("created_at").cast(TimestampType()),
    F.col("created_by").cast(StringType()),
    F.col("updated_at").cast(TimestampType()),
    F.col("updated_by").cast(StringType()),
)

# COMMAND ----------

# Insert into the target table
df_with_schema.write.mode("append").saveAsTable(f"{catalog_prefix}bronze.{schema_prefix}volue.curves_attributes")
