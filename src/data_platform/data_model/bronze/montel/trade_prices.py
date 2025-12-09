from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

trade_prices_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "montel", "trade_prices"),
    schema=StructType(
        [
            StructField("SymbolKey", StringType(), True),
            StructField("TradeId", LongType(), True),
            StructField("TradingTime", StringType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Volume", LongType(), True),
            StructField("TradeStatus", StringType(), True),
            StructField("OTC", BooleanType(), True),
            StructField("ModifiedTime", StringType(), True),
        ]
    ),
    sources=None,
    primary_keys=(),
)
