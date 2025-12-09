from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

power_generation_reanalysis_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteologica", "power_generation_reanalysis"),
    schema=StructType(
        [
            StructField("from_datetime", StringType(), False),
            StructField("to_datetime", StringType(), False),
            StructField("utc_offset_from", StringType(), True),
            StructField("utc_offset_to", StringType(), True),
            StructField("forecast", StringType(), True),
            StructField("observation", StringType(), True),
            StructField("installed_capacity_estimated", StringType(), True),
            StructField("installed_capacity_forecast", StringType(), True),
            StructField("content_id", LongType(), False),
            StructField("content_name", StringType(), True),
            StructField("installed_capacity", StringType(), True),
            StructField("issue_date", StringType(), False),
            StructField("timezone", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("update_id", StringType(), False),
        ]
    ),
    primary_keys=("update_id", "issue_date", "content_id", "from_datetime", "to_datetime"),
    sources=[FileVolumeIdentifier("bronze", "meteologica", "actual")],
)
