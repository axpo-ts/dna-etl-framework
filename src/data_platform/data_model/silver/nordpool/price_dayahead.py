from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.nordpool.nordpool_common import (
    NORDPOOL_COMMON_TAGS,
    NORDPOOL_LICENSE,
)

price_dayahead_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="nordpool",
        name="price_dayahead",
    ),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            standard_columns.DeliveryAreaColumn.to_struct_field(),
            StructField("price", DoubleType(), True, {"comment": "The spot price for the delivery period in the specified currency"}),
            StructField("price_currency", StringType(), True, {"comment": "The currency of the price, e.g. EUR, NOK, SEK, DKK"}),
            StructField("price_unit", StringType(), True, {"comment": "The unit of the price, e.g. EUR/MWh, NOK/MWh"}),
            StructField("buy_volume", DoubleType(), True, {"comment": "The buy volume for the delivery period in the specified unit"}),
            StructField("sell_volume", DoubleType(), True, {"comment": "The sell volume for the delivery period in the specified unit"}),
            StructField("volume_unit", StringType(), True, {"comment": "The unit of the volume, e.g. MWh"}),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
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
    license=NORDPOOL_LICENSE,
    tags={
        **NORDPOOL_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.SPOT,
    },
    primary_keys=("delivery_start", "delivery_end", "delivery_area"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
