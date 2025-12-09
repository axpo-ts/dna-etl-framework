from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

physical_flow_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "nordpool", "physical_flow"),
    schema=StructType(
        [
            StructField("deliveryArea", StringType(), True),
            StructField("deliveryStart", TimestampType(), True),
            StructField("deliveryEnd", TimestampType(), True),
            StructField("area", StringType(), True),
            StructField("connection", StringType(), True),
            StructField("import", DoubleType(), True),
            StructField("export", DoubleType(), True),
            StructField("netPosition", DoubleType(), True),
            StructField("market", StringType(), True),
            StructField("status", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("updatedAt", TimestampType(), True),
            StructField("deliveryDateCET", DateType(), True),
        ]
    ),
    sources=[FileVolumeIdentifier(catalog="staging", schema="nordpool", name="physical_flow")],
    primary_keys=(),
)
