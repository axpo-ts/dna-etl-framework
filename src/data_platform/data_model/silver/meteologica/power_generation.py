from __future__ import annotations

from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .common_variables import METEOLOGICA_COMMON_TAGS

power_generation_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="meteologica", name="power_generation"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("content_id", LongType(), False, {"comment": "Unique identifier for the content"}),
            StructField("value", StringType(), False, {"comment": "Observed value of power generation"}),
            standard_columns.UnitColumn.to_struct_field(),
            StructField("content_name", StringType(), False, {"comment": "Name of the content"}),
            StructField("update_id", StringType(), False, {"comment": "Identifier for the update batch"}),
            StructField("issue_date", StringType(), False, {"comment": "Timestamp when the data was issued"}),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
        ]
    ),
    comment="The table contains observations of power generation by different technology (e.g. wind, photovoltaic) across various countries.",
    sources=[TableIdentifier(catalog="bronze", schema="meteologica", name="power_generation")],
    primary_keys=("update_id", "issue_date", "content_id", "delivery_start"),
    partition_cols=("update_id",),
    tags={
        **METEOLOGICA_COMMON_TAGS,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION,
    },
)
