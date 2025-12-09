from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

commodity_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="ebx", name="commodity"),
    schema=StructType(
        [
            StructField("source_created_at", StringType(), True, {}),
            StructField("source_created_by", StringType(), True, {}),
            StructField("display_name", StringType(), True, {}),
            StructField("last_published_cet", StringType(), True, {}),
            StructField("level0", StringType(), True, {}),
            StructField("level1", StringType(), True, {}),
            StructField("level2", StringType(), True, {}),
            StructField("pk_commodity", StringType(), False, {}),
            StructField("source_updated_at", StringType(), True, {}),
            StructField("source_updated_by", StringType(), True, {}),
            StructField("_rescued_data", StringType(), True, {}),
        ]
    ),
)
