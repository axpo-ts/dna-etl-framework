from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

reanalysis_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteologica", "reanalysis"),
    schema=StructType(
        [
            StructField("from_datetime", StringType(), True),
            StructField("to_datetime", StringType(), True),
            StructField("utc_offset_from", StringType(), True),
            StructField("utc_offset_to", StringType(), True),
            StructField("forecast", StringType(), True),
            StructField("installed_capacity_forecast", StringType(), True),
            StructField("installed_capacity_estimated", StringType(), True),
            StructField("observation", StringType(), True),
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
    sources=[
        FileVolumeIdentifier("staging", "meteologica", f"meteologica/{content_id}/")
        for content_id in ["292", "302", "331", "386", "690", "770"]
    ],
)
