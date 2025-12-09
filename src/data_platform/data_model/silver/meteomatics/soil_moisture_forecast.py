from __future__ import annotations

from pyspark.sql.types import StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .meteomatics_common import METEOMATICS_COMMON_TAGS, meteomatics_standard_columns

meteomatics_soil_moisture_forecast_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="meteomatics", name="soil_moisture_forecast"),
    schema=StructType(
        [
            meteomatics_standard_columns.CurveNameColumn.to_struct_field(),
            standard_columns.DeliveryStartColumn.to_struct_field(comment="The start time of the forecasted period"),
            standard_columns.DeliveryEndColumn.to_struct_field(comment="The end time of the forecasted period"),
            standard_columns.DurationColumn.to_struct_field(comment="The duration of the forecasted period, e.g., '1h', '3h', '6h'"),
            meteomatics_standard_columns.CurveMemberColumn.to_struct_field(),
            meteomatics_standard_columns.ValueColumn.to_struct_field(comment="The forecasted value of the weather variable"),
            meteomatics_standard_columns.LatitudeColumn.to_struct_field(comment="The latitude of the location for which the forecast is made"),
            meteomatics_standard_columns.LongitudeColumn.to_struct_field(comment="The longitude of the location for which the forecast is made"),
            standard_columns.UnitColumn.to_struct_field(),
            meteomatics_standard_columns.ModelColumn.to_struct_field(comment="The weather model used for the forecast, e.g., 'GFS', 'ECMWF'"),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            meteomatics_standard_columns.RequestedTimeColumn.to_struct_field(),
        ]
    ),
    comment="The forecasted soil moisture index which indicates the wetness of the soil. This index is computed using the permanent wilting point and the field capacity, which both depend on the geographical location (soil type).",
    sources=[
        TableIdentifier(catalog="bronze", schema="meteomatics", name="forecast_meteomatics"),
        TableIdentifier(catalog="silver", schema="meteomatics", name="attribute"),
    ],
    tags={
        **METEOMATICS_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.REMI_JANNER,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.WEATHER,
        },
    primary_keys=("latitude", "delivery_start", "longitude", "delivery_end", "curve_name"),
    partition_cols=(),
    liquid_cluster_cols=("curve_name", "delivery_start", "latitude", "longitude"),
)
