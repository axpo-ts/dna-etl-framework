from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

bids_schema = StructType(
    [
        StructField("auctionID", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("corridorCode", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("awardedQuantity", DoubleType(), True),
        StructField("awardedPrice", DoubleType(), True),
        StructField("resoldQuantity", DoubleType(), True),
        StructField("productIdentification", StringType(), True),
        StructField("productHour", StringType(), True),
        StructField("productMinutesDelivered", IntegerType(), True),
        StructField("lastDataUpdate", TimestampType(), True),
    ]
)

bids_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "jao", "bids"),
    schema=bids_schema,
    sources=[FileVolumeIdentifier(catalog="staging", schema="jao", name="bids")],
    primary_keys=(),
)
