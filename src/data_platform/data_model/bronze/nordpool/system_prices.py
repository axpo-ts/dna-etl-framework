from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

system_prices_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "nordpool", "system_prices"),
    schema=StructType(
        [
            StructField("deliveryStart", TimestampType(), True),
            StructField("deliveryEnd", TimestampType(), True),
            StructField("price", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("exchangeRate", DoubleType(), True),
            StructField("marketMainCurrency", StringType(), True),
            StructField("averagePrice", DoubleType(), True),
            StructField("minPrice", DoubleType(), True),
            StructField("maxPrice", DoubleType(), True),
            StructField("updatedAt", TimestampType(), True),
            StructField("deliveryDateCET", DateType(), True),
        ]
    ),
    sources=[FileVolumeIdentifier(catalog="staging", schema="nordpool", name="system_spot")],
    primary_keys=(),
)
