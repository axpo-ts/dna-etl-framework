from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

trading_volume_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "nordpool", "trading_volume"),
    schema=StructType(
        [
            StructField("deliveryArea", StringType(), True),
            StructField("deliveryStart", TimestampType(), True),
            StructField("deliveryEnd", TimestampType(), True),
            StructField("buy", DoubleType(), True),
            StructField("sell", DoubleType(), True),
            StructField("market", StringType(), True),
            StructField("status", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("updatedAt", TimestampType(), True),
            StructField("deliveryDateCET", DateType(), True),
        ]
    ),
    sources=[FileVolumeIdentifier(catalog="staging", schema="nordpool", name="trading_volume")],
    primary_keys=(),
)
