from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

factor_discount_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="mds", name="factor_discount"),
    schema=StructType([
        StructField("mdo_id", LongType(), False, {"comment": "A unique identifier of the market data object (MDO) in MDS"}),
        StructField("mdo_name", StringType(), True, {"comment": "The name of the market data object (MDO)"}),
        StructField("reference_timestamp", TimestampType(), False, {"comment": "The datetime when the time series values have been created or published in UTC"}),
        StructField("delivery_start", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("delivery_end", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("duration", StringType(), True, {"comment": "The time resolution in a standardized ISO 8601 duration format e.g. PT1H"}),
        StructField("value", DoubleType(), True, {"comment": "Discount factor for the specified delivery period as of the reference_time"}),
        standard_columns.UnitColumn.to_struct_field(),
        StructField("relative_delivery_period", IntegerType(), True, {"comment": ""}),
        StructField("geolocation", StringType(), True, {"comment": "The geographic location associated with the data"}),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.RefCommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        StructField("data_provider", StringType(), True, {"comment": "The internal or external system, application, or platform we ingest data from"}),
    ]),
    comment="Discount factor curves for various delivery periods in different currencies as modelled by Axpo Trading.",
    sources=[
        TableIdentifier(catalog="bronze", schema="mds", name="egl_che_curve_discount_factor_curve"),
        TableIdentifier(catalog="silver", schema="mds", name="attribute"),
    ],
    primary_keys=("delivery_start", "mdo_id", "reference_timestamp", "delivery_end"),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.CARL_KEVIN_BRUNAES,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.MARKET_ANALYTICS_AND_VALUATION,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
