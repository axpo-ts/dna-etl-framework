from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

timeseriesbycontractsview = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="elviz_no",
        name="time_series_by_contracts",
    ),
    schema=StructType(
        [
            StructField("report_id", IntegerType(), True, {"comment": "A unique identifier for the report"}),
            StructField("contract_export_id", IntegerType(), True, {"comment": "A unique identifier of the exported contract record"}),
            StructField("time_series_alias", StringType(), True, {"comment": "A label of the time series e.g. Realized, Unrealized"}),
            StructField("from_date_time", TimestampType(), True, {"comment": "Local timestamp marking the start of the delivery period"}),
            StructField("from_date_time_utc", TimestampType(), True, {"comment": "UTC timestamp equivalent of the start of the delivery period"}),
            StructField("resolution", StringType(), True, {"comment": "The granularity of the time series"}),
            StructField("time_series_time_zone", StringType(), True, {"comment": "The timezone of the time series"}),
            StructField("load_type", StringType(), True, {"comment": "Classification of the load e.g. Base"}),
            StructField("load_hours", DoubleType(), True, {"comment": "Number of hours represented by the load profile segment"}),
            StructField("value_type", StringType(), True, {"comment": "The type of value"}),
            StructField("value_unit", StringType(), True, {"comment": "The unit of the value"}),
            StructField("value", DoubleType(), True, {"comment": "Numeric measureent for the corresponding interval"}),
            StructField("fee_type", StringType(), True, {"comment": "The type of fee"}),
            StructField("price_basis_name", StringType(), True, {"comment": "Basis used for pricing"}),
            StructField("report_date", DateType(), True, {"comment": "Date of report"}),
            standard_columns.LicenseColumn.to_struct_field(),
        ]
    ),
    comment="Time Series by Contracts View from Elviz",
    sources=(TableIdentifier(catalog="bronze", schema="elviz_no", name="timeseriesbycontractsview"),),
    primary_keys=(),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.VALUATION_AND_RISK,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.PER_SIVERSEN,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
