# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "reference"

tables = {
    "unit": [
        "id",
        "name",
        "active",
        "factor_standard_unit",
        "standard_unit",
        "type",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ],
    "unittype": ["id", "name", "active", "created_at", "created_by", "updated_at", "updated_by", "table_id"],
    "language": [
        "is_relevant_for_correspondence_language",
        "english",
        "alpha2",
        "alpha3_b",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ],
    "currency": [
        "currency_key",
        "currency_name",
        "is_complimentary",
        "is_fund",
        "is_metal",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ],
    "countryext": [
        "capital",
        "continent",
        "country_name",
        "ds",
        "dial",
        "dial_with_plus",
        "edgar",
        "fifa",
        "fips",
        "gaul",
        "geo_name_id",
        "ioc",
        "iso3166_1_alpha_2",
        "iso3166_1_alpha_3",
        "iso4217_currency_alphabetic_code",
        "iso4217_currency_country_name",
        "iso4217_currency_minor_unit",
        "iso4217_currency_name",
        "iso4217_currency_numeric_code",
        "itu",
        "is_independent",
        "languages",
        "m49",
        "marc",
        "max_zip_code_length",
        "min_zip_code_length",
        "official_name_english",
        "tld",
        "vat_max_length",
        "vat_min_length",
        "vat_prefix",
        "wmo",
        "postalformat",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ],
    "commodity": [
        "commodity_id",
        "commodity_name",
        "is_relevant_for_agreements",
        "is_relevant_for_es_contact",
        "is_relevant_for_legal_entities",
        "is_relevant_for_mandate_market",
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
