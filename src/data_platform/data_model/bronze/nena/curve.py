from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

curve_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "nena", "curve"),
    schema=StructType(
        [
            StructField("Code", StringType(), True),
            StructField("Area", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("Scenario", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Unit", StringType(), True),
            StructField("Resolution", StringType(), True),
            StructField("AnalyzedTime", TimestampType(), True),
            StructField("Timestamp", TimestampType(), True),
            StructField("Value", DoubleType(), True),
            StructField("UpdateTime", TimestampType(), True),
        ]
    ),
    sources=None,
    primary_keys=(),
)
