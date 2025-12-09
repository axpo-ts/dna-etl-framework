from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

metadata_curve_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "nena", "metadata_curve"),
    schema=StructType(
        [
            StructField("Code", StringType(), True),
            StructField("Area", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Unit", StringType(), True),
            StructField("Resolution", StringType(), True),
        ]
    ),
    sources=None,
    primary_keys=(),
)
