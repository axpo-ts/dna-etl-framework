# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "deal_data"

tables = {
    "ckw_contract": [
        "plant_number",
        "serviceprovider_id",
        "serviceprovidername",
        "serviceart_id",
        "serviceart_name",
        "extract_date",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ],
    "endur_power_volume_delta": [
        "timestamp",
        "loss_factor",
        "volume",
        "price",
        "volume_delta",
        "price_delta",
        "period_id",
        "deal_number",
        "transaction_number",
        "transaction_version",
        "transaction_status_id",
        "transaction_status",
        "instrument_type_id",
        "instrument_type",
        "base_instrument_type_id",
        "base_instrument_type",
        "buy_sell_id",
        "buy_sell",
        "internal_portfolio_id",
        "internal_portfolio",
        "internal_legal_entity_id",
        "internal_legal_entity",
        "internal_business_unit_id",
        "internal_business_unit",
        "internal_contact_id",
        "external_portfolio_id",
        "external_portfolio",
        "external_legal_entity_id",
        "external_legal_entity",
        "external_business_unit_id",
        "external_business_unit",
        "parameter_sequence_number",
        "pay_receive_id",
        "pay_receive",
        "settlement_type_id",
        "settlement_type",
        "projection_index_id",
        "projection_index",
        "location_id",
        "location_name",
        "control_area_name",
        "region_name",
        "schedule_id",
        "volume_type_id",
        "volume_type",
        "is_bav",
        "source_file_name",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ],
}

for table_name, columns in tables.items():
    # Get existing columns for the table
    table_path = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table_name}"
    existing_columns = set(col.name for col in spark.table(table_path).schema)

    # Check if all required columns exist
    missing_columns = [col for col in columns if col not in existing_columns]
    if missing_columns:
        raise ValueError(f"Table {table_path} is missing columns: {missing_columns}")

    # Proceed with column reordering
    for i, column in enumerate(columns):
        position_clause = "FIRST" if i == 0 else f"AFTER {columns[i - 1]}"
        sql_query = f"ALTER TABLE {table_path} ALTER COLUMN {column} {position_clause}"
        print(sql_query)
        spark.sql(sql_query)
