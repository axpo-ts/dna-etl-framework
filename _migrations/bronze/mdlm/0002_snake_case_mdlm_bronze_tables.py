# Databricks notebook source
import logging

from pyspark.sql import SparkSession

from data_platform.etl.transform.transform import snake_case_columns

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Get variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# Set script parameters
catalog = "bronze"
schema = "mdlm"
tables = ["lpi_definition", "lpi_end_user_entitlement"]
primary_keys = {
    "lpi_definition": ["provider", "lpi", "lpi_name"],
    "lpi_end_user_entitlement": ["provider", "lpi", "login_name"],
}


# COMMAND ----------
def drop_table_constraint(table_catalog: str, table_schema: str, table_name: str) -> None:
    """Drops all constraints from a specified table in the given catalog and schema.

    Args:
        table_catalog (str): The catalog containing the table.
        table_schema (str): The schema containing the table.
        table_name (str): The name of the table from which constraints will be dropped.
    """
    df = spark.sql(f"""
        SELECT constraint_name
        FROM {table_catalog}.information_schema.constraint_table_usage
        WHERE table_catalog = '{table_catalog}'
          AND table_schema = '{table_schema}'
          AND table_name = '{table_name}'
    """)
    constraints = [row["constraint_name"] for row in df.collect()]
    print(f"Found {constraints} constraints on table {table_catalog}.{table_schema}.{table_name}")
    for constraint in constraints:
        print(f"Dropping constraint {constraint}")
        spark.sql(f"ALTER TABLE `{table_catalog}`.`{table_schema}`.`{table_name}` DROP CONSTRAINT {constraint}")


# COMMAND ---------- Compose target table string
logger = logging.getLogger("")
for table_name in tables:
    table_catalog = f"{catalog_prefix}{catalog}"
    table_schema = f"{schema_prefix}{schema}"

    # Step 0: Drop constraints from main table
    drop_table_constraint(table_catalog, table_schema, table_name)

    # Step 1: Rename original table to _backup
    spark.sql(f"""
        ALTER TABLE {table_catalog}.{table_schema}.{table_name}
        RENAME TO {table_catalog}.{table_schema}.{table_name}_backup
    """)

    # Step 2: Read backup, convert columns to snake_case
    df = spark.table(f"{table_catalog}.{table_schema}.{table_name}_backup")
    df_snake = snake_case_columns(df, logger)
    df_snake = df_snake.withColumnRenamed("rescued_data", "_rescued_data")

    # Step 3: Create new table with NOT NULL constraints and primary key
    not_null_cols = [
        (
            f"{field.name} {field.dataType.simpleString()} NOT NULL"
            if not field.nullable
            else f"{field.name} {field.dataType.simpleString()}"
        )
        for field in df_snake.schema.fields
    ]
    pk_cols = primary_keys.get(table_name, [])
    pk_constraint = f"CONSTRAINT pk_{table_name} PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""
    create_stmt = f"""
        CREATE TABLE {table_catalog}.{table_schema}.{table_name} (
            {", ".join(not_null_cols)}
            {"," if pk_constraint else ""}
            {pk_constraint}
        )
    """
    print(create_stmt)
    spark.sql(create_stmt)

    # Step 4: Write data to new table
    df_snake.write.format("delta").mode("append").saveAsTable(f"{table_catalog}.{table_schema}.{table_name}")

    # Step 5: Tidy up - drop backup table
    spark.sql(f"DROP TABLE IF EXISTS {table_catalog}.{table_schema}.{table_name}_backup")
