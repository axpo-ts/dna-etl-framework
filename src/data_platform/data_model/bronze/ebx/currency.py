from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

currency_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="ebx", name="currency"),
    schema=StructType(
        [
            StructField("code", StringType(), True, {}),
            StructField("source_created_at", StringType(), True, {}),
            StructField("source_created_by", StringType(), True, {}),
            StructField("last_published_cet", StringType(), True, {}),
            StructField("name", StringType(), True, {}),
            StructField("pk_currency", StringType(), False, {}),
            StructField("source_updated_at", StringType(), True, {}),
            StructField("source_updated_by", StringType(), True, {}),
            StructField("_rescued_data", StringType(), True, {}),
        ]
    ),
)
