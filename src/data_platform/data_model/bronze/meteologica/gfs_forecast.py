from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

gfs_forecast_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteologica", "gfs_forecast"),
    schema=StructType(
        [
            StructField("from_datetime", StringType(), True),
            StructField("to_datetime", StringType(), True),
            StructField("utc_offset_from", StringType(), True),
            StructField("utc_offset_to", StringType(), True),
            StructField("forecast", StringType(), True),
            StructField("content_id", LongType(), True),
            StructField("content_name", StringType(), True),
            StructField("installed_capacity", StringType(), True),
            StructField("issue_date", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("update_id", StringType(), True),
        ]
    ),
    primary_keys=("update_id", "issue_date", "content_id", "from_datetime", "to_datetime"),
    sources=[FileVolumeIdentifier("staging", "meteologica", f"meteologica/{id}") for id in ["347", "422"]],
)
