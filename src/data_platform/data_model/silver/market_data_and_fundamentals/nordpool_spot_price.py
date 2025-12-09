from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

license = "NORDPOOL_STD_NORDPOOL"


nordpool_spot_price_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="nordpool_spot_price",
    ),
    schema=StructType(
        [
            StructField(
                "delivery_start",
                TimestampType(),
                True,
                {"comment": "The start of the delivery period for the contract, in the format YYYY-MM-DD HH:MM:SS"},
            ),
            StructField(
                "delivery_end",
                TimestampType(),
                True,
                {"comment": "The end of the delivery period for the contract, in the format YYYY-MM-DD HH:MM:SS"},
            ),
            StructField(
                "duration",
                StringType(),
                True,
                {"comment": "The duration of the delivery period, PT1H for 1 hour, PT15M for 15 minutes, etc."},
            ),
            StructField(
                "delivery_area",
                StringType(),
                True,
                {"comment": "The delivery area for the price, e.g. SE1, FI, DK1, etc."},
            ),
            StructField(
                "price",
                DoubleType(),
                True,
                {"comment": "The spot price for the delivery period in the specified currency"},
            ),
            StructField(
                "price_currency",
                StringType(),
                True,
                {"comment": "The currency of the price, e.g. EUR, NOK, SEK, DKK"},
            ),
            StructField(
                "price_unit",
                StringType(),
                True,
                {"comment": "The unit of the price, e.g. EUR/MWh, NOK/MWh"},
            ),
            StructField(
                "buy_volume",
                DoubleType(),
                True,
                {"comment": "The buy volume for the delivery period in the specified unit"},
            ),
            StructField(
                "sell_volume",
                DoubleType(),
                True,
                {"comment": "The sell volume for the delivery period in the specified unit"},
            ),
            StructField(
                "volume_unit",
                StringType(),
                True,
                {"comment": "The unit of the volume, e.g. MWh"},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
            StructField(
                "updated_at_source",
                TimestampType(),
                True,
                {"comment": "The timestamp of the last update from the source"},
            ),
        ]
    ),
    comment=(
        "This table contains spot prices and corresponding volumes for Nordpool markets. "
        "The data includes day-ahead market prices for different delivery areas along with buy and sell volumes."
    ),
    sources=[
        TableIdentifier(catalog="bronze", schema="nordpool", name="spot_prices"),
        TableIdentifier(catalog="bronze", schema="nordpool", name="trading_volume"),
        TableIdentifier(catalog="bronze", schema="nordpool", name="system_turnover"),
        TableIdentifier(catalog="bronze", schema="nordpool", name="system_prices"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "Nordpool",
    },
    primary_keys=("delivery_start", "delivery_area"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
