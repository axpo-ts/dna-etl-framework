# Databricks notebook source
import json

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore
schema = "mds"

tables = [
    "baltex_curve_price_index",
    "argus_curve_price_index",
    "apx_curve_auction_clearing",
    "belpex_curve_auction_clearing",
    "gme_curve_auction_clearing",
    "omipolo_curve_auction_clearing",
    "polpx_pl_curve_auction_clearing",
    "epex_pos_curve_auction_index",
]


def get_table_ddl_script(catalog: str, schema: str, target_table: str) -> str:
    """Generate the DDL script for creating a table with an auto-increment identity column.

    Args:
        catalog (str): The catalog name.
        schema (str): The schema name.
        target_table (str): The target table name.

    Returns:
        str: The DDL script for creating the table.
    """
    src_table = f"{catalog}.{schema}.{target_table}"
    df = spark.sql(
        f"select column_name from `system_tables_sharing`.system_tables_information_schema.constraint_column_usage "
        f"where lower(table_name) = '{target_table}' and lower(table_schema) = '{schema}' "
        f"and lower(table_catalog) = '{catalog}' and ("
        f"lower(constraint_name) = 'pk_{target_table}' or "
        f"lower(constraint_name) = '{target_table}_pk')"
    ).collect()
    pk = [row.column_name for row in df]
    result = []
    primary_key_str = ""
    if any(pk):
        primary_key_str = f", CONSTRAINT pk_{target_table} PRIMARY KEY({', '.join(pk)})"
    table_id_prefix_clause = ""
    # Retrieve schema from schema(if provided in dlt config), otherwise from source table
    schema = spark.read.table(src_table).schema.json()
    schema_json = json.loads(schema)

    # Build the DDL string from schema fields
    for metadata in schema_json["fields"]:
        if "table_id" not in metadata["name"] and metadata["name"] != "Volume":
            # Change ReferenceTime to TIMESTAMP type
            if metadata["name"] == "ReferenceTime":
                field_type = "timestamp"
            elif isinstance(metadata["type"], dict):
                field_type = f"{metadata['type']['type']}<{metadata['type']['elementType']}>"
            else:
                field_type = metadata["type"]
            not_nullable = " NOT NULL" if metadata["name"] in pk else ""
            comment = f" COMMENT '{metadata['metadata']['comment']}'" if "comment" in metadata["metadata"] else ""
            result.append(f"{metadata['name']} {field_type}{not_nullable}{comment}")

    return table_id_prefix_clause + ", ".join(result) + primary_key_str


def get_table_comment(table_name: str) -> str:
    """Retrieves the comment associated with a specified table in Spark.

    Args:
        table_name (str): The name of the table for which to retrieve the comment.

    Returns:
        str: The comment of the table in the format "COMMENT 'comment_text'",
                or an empty string if no comment is found.
    """
    tbl_metadata = spark.sql(f"DESCRIBE EXTENDED {table_name}").filter('col_name = "Comment"').first()
    table_comment = f"COMMENT '{tbl_metadata.data_type}'" if tbl_metadata and tbl_metadata.data_type else ""
    return table_comment


for table in tables:
    full_table = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{table}"
    if spark.catalog.tableExists(full_table):
        print(f"processing table {table}")
        columns = spark.catalog.listColumns(full_table)
        reference_time_column = next((col for col in columns if col.name == "ReferenceTime"), None)
        if (reference_time_column and reference_time_column.dataType != "timestamp") or any(
            col.name == "Volume" for col in columns
        ):
            ddl = get_table_ddl_script(f"{catalog_prefix}bronze", f"{schema_prefix}{schema}", table)
            tbl_comment = get_table_comment(full_table)
            query = f"""CREATE TABLE IF NOT EXISTS {full_table} (
                    {ddl}
                ) {tbl_comment}
                TBLPROPERTIES(
                    'delta.feature.allowColumnDefaults' = 'supported',
                    'delta.columnMapping.mode' = 'name'
                )"""
            print(query)
            dbutils.fs.rm(
                f"/Volumes/{catalog_prefix}staging/{schema_prefix}{schema}/{table}/checkpoint/schemaLocation/", True
            )  # type: ignore F821
            spark.sql(f"ALTER TABLE {full_table} DROP CONSTRAINT IF EXISTS pk_{table}")
            spark.sql(f"ALTER TABLE {full_table} RENAME TO {full_table}_backup")
            spark.sql(query)
            (
                spark.read.table(f"{full_table}_backup")
                .drop("table_id", "Volume")
                .withColumn("ReferenceTime", f.to_timestamp(f.col("ReferenceTime")))
                .write.format("delta")
                .mode("append")
                .saveAsTable(full_table)
            )
            tags_df = spark.sql(f"""SELECT tag_name, tag_value FROM system.information_schema.table_tags
                                WHERE catalog_name = '{catalog_prefix}bronze'
                                AND schema_name = '{schema_prefix}{schema}'
                                AND table_name = '{table}_backup'""")
            for row in tags_df.collect():
                tag_name = row["tag_name"]
                tag_value = row["tag_value"]
                spark.sql(
                    f"""
                    ALTER TABLE {catalog_prefix}bronze.{schema_prefix}{schema}.{table}
                    SET TAGS ("{tag_name}" = '{tag_value}')
                    """
                )
            spark.sql(f"DROP TABLE {full_table}_backup")


schema_removal_tables = ["ice_curve_eod_settlement", "opcom_curve_auction_clearing"]
for table in schema_removal_tables:
    dbutils.fs.rm(f"/Volumes/{catalog_prefix}staging/{schema_prefix}{schema}/{table}/checkpoint/schemaLocation/", True)


opcom_clear_table = "opcom_curve_auction_clearing"
files = dbutils.fs.ls(f"/Volumes/{catalog_prefix}staging/{schema_prefix}{schema}/{opcom_clear_table}")
for file in files:
    if file.name.startswith("112100251"):
        dbutils.fs.rm(f"/Volumes/{catalog_prefix}staging/{schema_prefix}{schema}/{opcom_clear_table}/{file.name}", True)
spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}{schema}.{opcom_clear_table}")


ice_curve_clear_files = "ice_curve_eod_settlement"
files = dbutils.fs.ls(f"/Volumes/{catalog_prefix}staging/{schema_prefix}{schema}/{ice_curve_clear_files}")
for file in files:
    if file.name.startswith("88019751"):
        dbutils.fs.rm(f"/Volumes/{catalog_prefix}staging/{schema_prefix}{schema}/{opcom_clear_table}/{file.name}", True)
