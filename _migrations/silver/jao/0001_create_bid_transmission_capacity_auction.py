# Databricks notebook source

from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    element_at,
    expr,
    lit,
    regexp_extract,
    regexp_replace,
    size,
    split,
    to_utc_timestamp,
    trim,
    try_to_timestamp,
    when,
)

from data_platform.data_model.silver.jao import bid_transmission_capacity_auction_table
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


CreateTable(context=context, table_model=bid_transmission_capacity_auction_table).execute()


def adjust_for_dst(start_timestamp: Column, end_timestamp: Column) -> tuple[Column, Column]:
    """Adjusting back DST timestamps.

    Detects the collapsed hour after converting to timestamp from DST.
    Reconstructing its true start by subtracting an hour.
    Then applying logic to detect and shift on the asterix for the autumn DST end.
    Returns the expression for the timestamp processing, to be applied in a data frame select.
    TODO: Current weakpoint is that UTC conversion happens after the 24:00 -> 23:59 conversion,
    which can result in 22:59 or 00:59

    Returns:
        tuple[Column, Column]: start_timestamp, end_timestamp
    """
    # Flatspots due to to_utc_timestamp conversion
    is_flat = start_timestamp == end_timestamp

    # Take the single UTC start instant and stretch it back 1h for the flatspot
    adj_start = when(is_flat, end_timestamp - expr("INTERVAL 1 HOUR")).otherwise(start_timestamp)

    # Handling the autumn "double hour"
    start_time_expr = when(col("product_hour").endswith("*"), adj_start + expr("INTERVAL 1 HOUR")).otherwise(adj_start)

    # End hour is bumped forward one hour if going into non-DST time
    end_time_expr = when(col("product_hour").endswith("*"), end_timestamp + expr("INTERVAL 1 HOUR")).otherwise(
        end_timestamp
    )
    return start_time_expr, end_time_expr


# COMMAND ----------
# Product hour DST starts as a gap, then becomes a duplicated product hour with a * (star)

# strip off any "*" for a clean hour range, and extract anything else other than the hour range
cleaned_hour = regexp_extract(col("product_hour"), r"(\d{2}:\d{2}-\d{2}:\d{2})", 1)
raw_comment = regexp_replace(col("product_hour"), r"\b\d{2}:\d{2}-\d{2}:\d{2}\*?", "")

schedule_type = when(trim(raw_comment) == "", None).otherwise(trim(raw_comment))

parts = split(cleaned_hour, "-")
valid = size(parts) == 2  # invalid if couldnt find both start and end

# If invalid defaults to 00:00 (horizons with lower granularity)
start_hour = when(valid, element_at(parts, 1)).otherwise(lit("00:00"))
end_hour = when(valid, element_at(parts, 2)).otherwise(lit("00:00"))
end_hour = when(valid & (end_hour == "24:00"), lit("23:59")).otherwise(end_hour)

# bids - build UTC converted timestamps

bids_date_extract = element_at(split(col("auction_id"), "-"), -2)
bids_start_ts = to_utc_timestamp(
    try_to_timestamp(concat(bids_date_extract, lit(" "), start_hour), lit("yyMMdd HH:mm")), "Europe/Oslo"
)

bids_end_ts = to_utc_timestamp(
    try_to_timestamp(concat(bids_date_extract, lit(" "), end_hour), lit("yyMMdd HH:mm")), "Europe/Oslo"
)

bids_delivery_start_expr, bids_delivery_end_expr = adjust_for_dst(bids_start_ts, bids_end_ts)

src_table_name = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.jao_bids"

df = spark.read.format("delta").table(src_table_name)

bids_slv_df = df.select(
    col("auction_id"),
    bids_delivery_start_expr.alias("delivery_start"),
    bids_delivery_end_expr.alias("delivery_end"),
    lit("PT1H").alias("duration"),
    col("corridor_code"),
    col("from_area"),
    col("to_area"),
    col("product_identification"),
    col("product_minutes_delivered"),
    col("product_hour"),
    col("currency"),
    col("price"),
    col("awarded_price"),
    col("quantity"),
    col("awarded_quantity"),
    col("resold_quantity"),
    col("cable_info"),
    col("updated_at_source"),
    lit("POWER").alias("commodity"),
    "license",
    lit("jao").alias("data_source"),
    lit("jao").alias("data_system"),
    col("created_at"),
    col("created_by"),
    col("updated_at"),
    col("updated_by"),
    col("table_id"),
)


bids_slv_df.createOrReplaceTempView("jao_bids_enriched")

load_query = f"""
INSERT INTO {catalog_prefix}silver.{schema_prefix}jao.bid_transmission_capacity_auction
SELECT *
FROM jao_bids_enriched
"""

result_df = spark.sql(load_query)
