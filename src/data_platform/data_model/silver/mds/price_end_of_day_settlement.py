from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

price_end_of_day_settlement_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="mds", name="price_end_of_day_settlement"),
    schema=StructType([
        StructField("mdo_id", LongType(), False, {"comment": "A unique identifier of the market data object (MDO) in MDS"}),
        StructField("mdo_name", StringType(), True, {"comment": "The name of the market data object (MDO)"}),
        StructField("reference_timestamp", TimestampType(), False, {"comment": "The datetime when the time series values have been created or published in UTC"}),
        StructField("delivery_start", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("delivery_end", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("duration", StringType(), True, {"comment": "The time resolution in a standardized ISO 8601 duration format e.g. PT1H"}),
        StructField("price", DoubleType(), True, {"comment": "The end-of-day (EOD) settlement prices for the specified product and delivery period, expressed in the specified unit (e.g., EUR/MWh)"}),
        standard_columns.UnitColumn.to_struct_field(),
        StructField("relative_delivery_period", IntegerType(), True, {"comment": ""}),
        StructField("geolocation", StringType(), True, {"comment": "The geographic location associated with the data"}),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.RefCommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        StructField("data_provider", StringType(), True, {"comment": "The internal or external system, application, or platform we ingest data from"}),
    ]),
    comment="The end-of-day (EOD) settlement prices for the specified product and delivery period, expressed in the specified unit (e.g., EUR/MWh)",
    sources=[
        TableIdentifier(catalog="bronze", schema="mds", name="ice_curve_eod_settlement"),
        TableIdentifier(catalog="silver", schema="mds", name="attribute"),
        TableIdentifier(catalog="bronze", schema="mds", name="endex_curve_eod_settlement"),
        TableIdentifier(catalog="bronze", schema="mds", name="ice_curve_eod_settlement_spread"),
        TableIdentifier(catalog="bronze", schema="mds", name="eex_curve_eod_settlement"),
    ],
    partition_cols=["data_source"],
    primary_keys=("delivery_end", "mdo_id", "delivery_start", "reference_timestamp"),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.VLADIMIR_SARAMET,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.SETTLEMENT,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
