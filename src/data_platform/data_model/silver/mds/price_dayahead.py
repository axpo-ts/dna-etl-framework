from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

price_dayahead_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="mds", name="price_dayahead"),
    schema=StructType([
        StructField("mdo_id", LongType(), False, {"comment": "A unique identifier of the market data object (MDO) in MDS"}),
        StructField("mdo_name", StringType(), True, {"comment": "The name of the market data object (MDO)"}),
        StructField("reference_timestamp", TimestampType(), False, {"comment": "The datetime when the time series values have been created or published in UTC"}),
        StructField("delivery_start", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("delivery_end", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("duration", StringType(), True, {"comment": "The time resolution in a standardized ISO 8601 duration format e.g. PT1H"}),
        StructField("price", DoubleType(), True, {"comment": "Day-ahead power market prices in the specified location, expressed in the specified unit (e.g., EUR/MWh)"}),
        standard_columns.UnitColumn.to_struct_field(),
        StructField("relative_delivery_period", IntegerType(), True, {"comment": ""}),
        StructField("geolocation", StringType(), True, {"comment": "The geographic location associated with the data"}),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.RefCommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        StructField("data_provider", StringType(), True, {"comment": "The internal or external system, application, or platform we ingest data from"}),
    ]),
    comment="Day-ahead power market prices in the specified location, expressed in the specified unit (e.g., EUR/MWh)",
    sources=[
       TableIdentifier(catalog="bronze",schema="mds",name="belpex_curve_auction_clearing"),
       TableIdentifier(catalog="silver",schema="mds",name="attribute"),
       TableIdentifier(catalog="bronze",schema="mds",name="gme_curve_auction_clearing"),
       TableIdentifier(catalog="bronze",schema="mds",name="apx_curve_auction_clearing"),
       TableIdentifier(catalog="bronze",schema="mds",name="epex_pos_curve_auction_clearing")
    ],
    primary_keys=("mdo_id", "reference_timestamp", "delivery_start", "delivery_end"),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.CHRISTOPHE_CATTRY,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.SPOT,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
