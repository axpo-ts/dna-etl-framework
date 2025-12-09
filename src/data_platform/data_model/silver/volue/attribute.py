from __future__ import annotations

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns

from .volue_common import VOLUE_ATTRIBUTE_COMMON_TAGS, VOLUE_ATTRIBUTE_LICENSE, volue_standard_columns

attribute_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="volue",
        name="attribute"
    ),
    comment="Attributes (e.g.,license, unit, etc.) that are associated with various entities within Volue.",
    schema=StructType([
        StructField("id", IntegerType(), True, {"comment": "A unique identifier for each curve"}),
        StructField("name", StringType(), True, {"comment": "The name of each curve"}),
        StructField("curve_type", StringType(), True, {"comment": "The type of the curve e.g. TIME_SERIES"}),
        StructField("curve_state", StringType(), True, {"comment": "The access state of the curve e.g. PUBLIC"}),
        volue_standard_columns.DataTypeColumn.to_struct_field(),
        volue_standard_columns.CategoriesColumn.to_struct_field(),
        standard_columns.DurationColumn.to_struct_field(),
        StructField("time_zone", StringType(), True, {"comment": "The time zone associated with the data"}),
        standard_columns.UnitColumn.to_struct_field(),
        volue_standard_columns.AreaColumn.to_struct_field(),
        standard_columns.CommodityColumn.to_struct_field(),
        StructField("created", StringType(), True, {"comment": "The timestamp indicating when the record or entry was initially created in the system"}),
        StructField("modified", StringType(), True, {"comment": "The timestamp indicating the most recent update or modification made to the record or entry"}),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
        StructField("description", StringType(), True, {"comment": "The description of the curve"}),
        standard_columns.ValidFromColumn.to_struct_field(),
        standard_columns.ValidToColumn.to_struct_field(),
        standard_columns.IsCurrentColumn.to_struct_field(),
        StructField("silver_table", StringType(), True, {"comment": "Contains the table name where the curve id is stored in the silver catalog."}),
    ]),
    sources=[
       TableIdentifier(catalog="bronze",schema="volue",name="curves_attributes")
    ],
    primary_keys=["id"],
    license=VOLUE_ATTRIBUTE_LICENSE,
    tags=VOLUE_ATTRIBUTE_COMMON_TAGS
)
