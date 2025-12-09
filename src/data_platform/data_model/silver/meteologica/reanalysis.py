from __future__ import annotations

from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .common_variables import METEOLOGICA_COMMON_TAGS

reanalysis_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="meteologica", name="reanalysis"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("content_id", LongType(), True, {"comment": "Unique identifier for the content"}),
            StructField("observation", StringType(), True, {"comment": "Observed value for the metric"}),
            StructField("forecast", StringType(), True, {"comment": "Forecasted value for the metric"}),
            standard_columns.UnitColumn.to_struct_field(nullable=True),
            StructField(
                "installed_capacity_forecast", StringType(), True, {"comment": "Forecasted installed capacity"}
            ),
            StructField(
                "installed_capacity_estimated", StringType(), True, {"comment": "Estimated installed capacity"}
            ),
            StructField("installed_capacity", StringType(), True, {"comment": "Actual installed capacity"}),
            StructField("content_name", StringType(), True, {"comment": "Name or description of the content"}),
            StructField("issue_date", StringType(), True, {"comment": "Date when the data was issued"}),
            StructField("timezone", StringType(), True, {"comment": "Timezone information for the data"}),
            StructField(
                "update_id", StringType(), True, {"comment": "Identifier for the update or version of the data"}
            ),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
        ]
    ),
    comment="The table contains forecasts and observations related to installed capacity over specified time periods. It includes details such as the forecasted and estimated installed capacity, along with timestamps and timezone information. This data can be used for analyzing capacity trends, comparing forecasts against actual observations, and understanding the impact of time zones on capacity metrics.",
    sources=[
        TableIdentifier(catalog="bronze", schema="meteologica", name="reanalysis"),
    ],
    primary_keys=("update_id", "issue_date", "content_id", "delivery_start"),
    partition_cols=("update_id",),
    tags={**METEOLOGICA_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION},
)
