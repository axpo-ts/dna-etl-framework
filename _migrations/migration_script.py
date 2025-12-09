# Databricks notebook source
import re

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Read parameters

catalog_name = dbutils.widgets.get("catalog_name")  # type: ignore # noqa: F821
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_name = dbutils.widgets.get("schema_name")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
table_name = dbutils.widgets.get("table_name")  # type: ignore # noqa: F821
migration_script_path = dbutils.widgets.get("migration_script_path")  # type: ignore # noqa: F821
azure_subscription_env = dbutils.widgets.get("azure_subscription_env")  # type: ignore # noqa: F821


# COMMAND ----------

# DBTITLE 1,Create migration table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_prefix}{catalog_name}.{schema_prefix}{schema_name}.{table_name} (
  migration_script STRING,
  created_at TIMESTAMP,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING
)
""")

# COMMAND ----------

# DBTITLE 1,List all migration scripts
_migrations = dbutils.fs.ls(f"file:{migration_script_path}/_migrations/{catalog_name}/{schema_name}")  # type: ignore # noqa: F821
_migrations

# COMMAND ----------

# DBTITLE 1,Read migration table
df = spark.read.table(f"{catalog_prefix}{catalog_name}.{schema_prefix}{schema_name}.{table_name}")


# COMMAND ----------

# DBTITLE 1,Function: add script to migration table


def insert_script(script_name: str) -> None:
    """Inserts script name if it has run in migration steps."""
    spark.sql(
        f"INSERT INTO {catalog_prefix}{catalog_name}.{schema_prefix}{schema_name}.{table_name} "
        f"VALUES ('{script_name}', current_timestamp(), current_user(), current_timestamp(), current_user())"
    )


# COMMAND ----------

# DBTITLE 1,Get scripts that already ran
_existing_scripts = df.select("migration_script").collect()
_existing_scripts = [item[0] for item in _existing_scripts]
_existing_scripts

# COMMAND ----------

# DBTITLE 1,Get all scripts
_scripts = [item[1] for item in _migrations]
# Remove .py on file to make backwards compatibile following 16.4
_scripts = [_script.replace(".py", "") for _script in _scripts]
_scripts

# COMMAND ----------

# DBTITLE 1,Find new (unrun) scripts
_scripts = list(set(_scripts) - set(_existing_scripts))
_scripts

# COMMAND ----------

# DBTITLE 1,Sort scripts by name
_scripts.sort(key=lambda test_string: next(map(int, re.findall(r"\d+", test_string))))

# COMMAND ----------

_scripts

# COMMAND ----------

# DBTITLE 1,Run the scripts in order
print(f"running migration for {schema_name} schema")
for notebook in _scripts:
    print(notebook)
    dbutils.notebook.run(  # type: ignore # noqa: F821
        f"./{catalog_name}/{schema_name}/{notebook}",
        0,
        {
            "env_catalog_identifier": catalog_prefix,
            "schema_prefix": schema_prefix,
            "azure_subscription_env": azure_subscription_env,
        },
    )  # type: ignore
    # insert without extension to avoid need to migrate _migration tables
    insert_script(notebook)
