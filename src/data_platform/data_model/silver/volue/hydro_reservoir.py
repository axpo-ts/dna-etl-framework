from __future__ import annotations

from pyspark.sql.types import DoubleType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .volue_common import VOLUE_COMMON_TAGS, VOLUE_LICENSE, volue_standard_columns, VOLUE_TIMESERIES_PRIMARY_KEYS

hydro_reservoir_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="volue",
        name="hydro_reservoir"
    ),
    schema=StructType([
        volue_standard_columns.CurveIdColumn.to_struct_field(),
        volue_standard_columns.CurveNameColumn.to_struct_field(nullable=False),
        standard_columns.DeliveryStartColumn.to_struct_field(nullable=False),
        standard_columns.DeliveryEndColumn.to_struct_field(),
        standard_columns.DurationColumn.to_struct_field(),
        StructField("value", DoubleType(), True, {"comment": "The actual measurement of hydro reservoir level, in the specified unit"}),
        standard_columns.UnitColumn.to_struct_field(),
        volue_standard_columns.DataTypeColumn.to_struct_field(),
        volue_standard_columns.AreaColumn.to_struct_field(),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.RefCommodityColumn.to_struct_field(),
        volue_standard_columns.CategoriesColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
        volue_standard_columns.VolueDescriptionColumn.to_struct_field()
    ]),
    comment=(
        "The actual measurements of hydro reservoir levels. Each curve maps directly to a single time series instance."
    ),
    sources=[
       TableIdentifier(catalog="bronze",schema="volue",name="time_series_n"),
       TableIdentifier(catalog="bronze",schema="volue",name="time_series_sa"),
       TableIdentifier(catalog="bronze",schema="volue",name="time_series_af")
    ],
    partition_cols=["curve_name"],
    primary_keys=VOLUE_TIMESERIES_PRIMARY_KEYS,
    license=VOLUE_LICENSE,
    tags={
        **VOLUE_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.HYDRO,
    }
)
