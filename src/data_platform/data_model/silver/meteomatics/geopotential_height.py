from __future__ import annotations

from pyspark.sql.types import StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .meteomatics_common import METEOMATICS_COMMON_TAGS, meteomatics_standard_columns

meteomatics_geopotential_height_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="meteomatics", name="geopotential_height"),
    schema=StructType(
        [
            meteomatics_standard_columns.CurveNameColumn.to_struct_field(),
            standard_columns.DeliveryStartColumn.to_struct_field(comment="The start time of the observed period"),
            standard_columns.DeliveryEndColumn.to_struct_field(comment="The end time of the observed period"),
            standard_columns.DurationColumn.to_struct_field(comment="The duration of the observed period"),
            meteomatics_standard_columns.ValueColumn.to_struct_field(),
            meteomatics_standard_columns.LatitudeColumn.to_struct_field(),
            meteomatics_standard_columns.LongitudeColumn.to_struct_field(),
            standard_columns.UnitColumn.to_struct_field(),
            meteomatics_standard_columns.ModelColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
        ]
    ),
    comment="The actual geopotential height in meters or decameters at the indicated level. The geopotential height differs from geometric height (elevation above sea level) and takes into account the variation of gravity with latitude and elevation.",
    sources=[
        TableIdentifier(catalog="bronze", schema="meteomatics", name="actuals"),
    ],
    tags={
        **METEOMATICS_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.CORSO_QUILICI,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.WEATHER,
        },
    primary_keys=("latitude", "delivery_start", "longitude", "delivery_end", "curve_name"),
    partition_cols=(),
    #liquid_cluster_cols=("curve_name", "delivery_start", "latitude", "longitude"),
    license="METEOMAT_WEATHER_API_METEOMAT"
)
