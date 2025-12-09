from __future__ import annotations

from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

attribute_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="meteomatics", name="attribute"),
    schema=StructType(
        [
            StructField("curve_name", StringType(), True, {"comment": "The name of each curve"}),
            StructField(
                "duration",
                StringType(),
                True,
                {"comment": "The duration in compliance with the ISO 8601 format e.g. P1D"},
            ),
            StructField("interval", StringType(), True, {"comment": "The interval of the data e.g. 24h"}),
            StructField("value_column", StringType(), True, {"comment": "The name of the value column"}),
            standard_columns.UnitColumn.to_struct_field(),
            StructField("model", StringType(), True, {"comment": "The model name associated with the curve"}),
            StructField("level", StringType(), True, {"comment": "The level associated with the curve e.g. 10hPa"}),
            StructField(
                "measure", StringType(), True, {"comment": "The measure associated with the curve e.g. mean, min"}
            ),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            StructField(
                "valid_from", TimestampType(), True, {"comment": "The start date from which the record is valid"}
            ),
            StructField("valid_to", TimestampType(), True, {"comment": "The end date until which the record is valid"}),
            StructField(
                "is_current",
                BooleanType(),
                True,
                {"comment": "Indicates whether the record is the current valid record"},
            ),
        ]
    ),
    comment="Attributes (e.g.,license, unit, commodity, etc.) that are associated with various entities within Meteomatics.",
    sources=[TableIdentifier(catalog="bronze", schema="meteomatics", name="attributes")],
    primary_keys=(),
    partition_cols=(),
    liquid_cluster_cols=(),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.YUFAN_HE,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
