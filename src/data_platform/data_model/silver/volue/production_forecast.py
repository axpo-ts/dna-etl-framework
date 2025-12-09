from __future__ import annotations

from pyspark.sql.types import DoubleType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .volue_common import VOLUE_COMMON_TAGS, VOLUE_LICENSE, volue_standard_columns

production_forecast_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="volue",
        name="production_forecast"
    ),
    schema=StructType([
        volue_standard_columns.CurveIdColumn.to_struct_field(),
        volue_standard_columns.CurveNameColumn.to_struct_field(nullable=False),
        volue_standard_columns.ReferenceDateColumn.to_struct_field(),
        standard_columns.DeliveryStartColumn.to_struct_field(nullable=False),
        standard_columns.DeliveryEndColumn.to_struct_field(),
        standard_columns.DurationColumn.to_struct_field(),
        volue_standard_columns.TagColumn.to_struct_field(),
        StructField("value", DoubleType(), True, {"comment": "The forecasted power production value in the specified unit"}),
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
        "The power production forecasts from Volue. Instance curves and tagged curves store multiple time series. "
        "An instance is identified with an issue date, holding the values for the particular forecast at the issue date. "
        "Tagged curves  normally store ensemble data and each time series in  a tagged curve is identified by a tag."
    ),
    sources=[
       TableIdentifier(catalog="bronze",schema="volue",name="time_series_f"),
       TableIdentifier(catalog="bronze",schema="volue",name="instances_f")
    ],
    partition_cols=["curve_name"],
    license=VOLUE_LICENSE,
    tags={
        **VOLUE_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.LOAD,
    }
)
