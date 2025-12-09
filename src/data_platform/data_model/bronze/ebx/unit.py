from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

unit_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="ebx", name="unit"),
    schema=StructType(
        [
            StructField("source_created_at", StringType(), True, {}),
            StructField("source_created_by", StringType(), True, {}),
            StructField("last_published_cet", StringType(), True, {}),
            StructField("long_name", StringType(), True, {}),
            StructField("pk_unit", StringType(), False, {}),
            StructField("short_name", StringType(), True, {}),
            StructField("type", StringType(), True, {}),
            StructField("source_updated_at", StringType(), True, {}),
            StructField("source_updated_by", StringType(), True, {}),
            StructField("_rescued_data", StringType(), True, {}),
        ]
    ),
)
