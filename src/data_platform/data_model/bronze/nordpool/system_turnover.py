from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

system_turnover_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "nordpool", "system_turnover"),
    schema=StructType(
        [
            StructField("deliveryStart", TimestampType(), True),
            StructField("deliveryEnd", TimestampType(), True),
            StructField("turnover", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("updatedAt", TimestampType(), True),
            StructField("deliveryDateCET", DateType(), True),
        ]
    ),
    sources=[FileVolumeIdentifier(catalog="staging", schema="nordpool", name="system_turnover")],
    primary_keys=(),
)
