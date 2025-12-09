from pyspark.sql import SparkSession


def get_tables_from_schema(
    spark: SparkSession,
    schema_name: str,
    exclusion_table_list: list[str] | None = None,
    exclude_system_table: bool = True,
) -> list[str]:
    """Get a list of table names in the given schema.

    Args:
        spark: SparkSession instance.
        schema_name: Name of the schema to list tables from.
        exclusion_table_list: Tables to exclude. Defaults to [].
        exclude_system_table: Whether to exclude tables starting with "_". Defaults to True.

    Returns:
        List of table names to process.
    """
    exclusion_table_list = exclusion_table_list or []

    return [
        t.name
        for t in spark.catalog.listTables(schema_name)
        if t.name not in exclusion_table_list and (not exclude_system_table or not t.name.startswith("_"))
    ]
