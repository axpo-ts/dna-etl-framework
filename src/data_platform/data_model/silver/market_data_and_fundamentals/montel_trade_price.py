from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

license = "DNA_MONTELNEWS_DATA_LICENSE"


montel_trade_price_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="montel_trade_price",
    ),
    schema=StructType(
        [
            StructField(
                "symbol_key",
                StringType(),
                False,
                {"comment": "The symbol key of the traded contract"},
            ),
            StructField(
                "trading_time",
                TimestampType(),
                False,
                {"comment": "The timestamp when the trade occurred"},
            ),
            StructField(
                "price",
                DoubleType(),
                True,
                {"comment": "The trade price"},
            ),
            StructField(
                "volume",
                LongType(),
                True,
                {"comment": "The trade volume"},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
        ]
    ),
    comment="This table contains Montel trades tick data for a wide range of commodity contracts.",
    sources=[
        TableIdentifier(catalog="bronze", schema="montel", name="trade_prices"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "Montel",
    },
    primary_keys=("symbol_key", "trading_time"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
