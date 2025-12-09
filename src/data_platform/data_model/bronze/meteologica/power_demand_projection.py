from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

power_demand_projection_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteologica", "power_demand_projection"),
    schema=StructType(
        [
            StructField("from_datetime", StringType(), False),
            StructField("to_datetime", StringType(), False),
            StructField("utc_offset_from", StringType(), False),
            StructField("utc_offset_to", StringType(), False),
            StructField("value", StringType(), False),
            StructField("content_id", LongType(), False),
            StructField("content_name", StringType(), False),
            StructField("issue_date", StringType(), False),
            StructField("timezone", StringType(), False),
            StructField("unit", StringType(), False),
            StructField("update_id", StringType(), False),
        ]
    ),
    primary_keys=("update_id", "issue_date", "content_id", "from_datetime", "to_datetime"),
    sources=[FileVolumeIdentifier("staging", "meteologica", f"meteologica/{id}") for id in ["315", "572", "875"]],
)
