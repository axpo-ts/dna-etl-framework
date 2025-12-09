from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

production_reanalysis_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="mds", name="production_reanalysis"),
    schema=StructType([
        StructField("mdo_id", LongType(), False, {"comment": "A unique identifier of the market data object (MDO) in MDS"}),
        StructField("mdo_name", StringType(), True, {"comment": "The name of the market data object (MDO)"}),
        StructField("reference_timestamp", TimestampType(), False, {"comment": "The datetime when the time series values have been created or published in UTC"}),
        StructField("delivery_start", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("delivery_end", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("duration", StringType(), True, {"comment": "The time resolution in a standardized ISO 8601 duration format e.g. PT1H"}),
        StructField("eic", DoubleType(), True, {"comment": "The estimated installed capacity"}),
        StructField("real_data", DoubleType(), True, {"comment": "The real time production"}),
        StructField("fic", DoubleType(), True, {"comment": "The current installed capacity"}),
        StructField("p50_fc", DoubleType(), True, {"comment": "The modelled production adjusted on the current installed capacity"}),
        standard_columns.UnitColumn.to_struct_field(),
        StructField("relative_delivery_period", IntegerType(), True, {"comment": ""}),
        StructField("geolocation", StringType(), True, {"comment": "The geographic location associated with the data"}),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.RefCommodityColumn.to_struct_field(),
        StructField("technology", StringType(), True, {"comment": ""}),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        StructField("data_provider", StringType(), True, {"comment": "The internal or external system, application, or platform we ingest data from"}),
    ]),
    comment="The blend of historical observations and model outputs of power production (reanalysis).",
    sources=[
        TableIdentifier(catalog="bronze", schema="mds", name="meteolog_curves_power_production"),
        TableIdentifier(catalog="silver", schema="mds", name="attribute"),
    ],
    primary_keys=("reference_timestamp", "mdo_id", "delivery_end", "delivery_start"),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.CORSO_QUILICI,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
