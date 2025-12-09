"""Hydro Inflow curves table model."""

from __future__ import annotations

from pyspark.sql.types import StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.nena import nena_common

nena_standard_columns = nena_common

license = "DNA-ALL-ACCESS"

hydro_inflow_table = StaticTableModel(
    identifier=TableIdentifier("silver", "nena", "hydro_inflow"),
    schema=StructType([
        nena_standard_columns.NameColumn.to_struct_field(),
        nena_standard_columns.AreaColumn.to_struct_field(),
        nena_standard_columns.ModelColumn.to_struct_field(),
        nena_standard_columns.ScenarioColumn.to_struct_field(),
        nena_standard_columns.ResolutionColumn.to_struct_field(),
        nena_standard_columns.TimestampColumn.to_struct_field(),
        nena_standard_columns.ValueColumn.to_struct_field(),
        standard_columns.UnitColumn.to_struct_field(comment="Unit"),
        nena_standard_columns.DescriptionColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(comment="License identifier"),
    ]),
    comment="Actual and forecast timeseries for water inflow to reservoirs pr area from Nena (GWh).",
    sources=[
        TableIdentifier(catalog="bronze", schema="nena", name="curve"),
        TableIdentifier(catalog="bronze", schema="nena", name="metadata_curve")
    ],
    license=license,
    tags={
        **nena_common.NENA_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.HYDRO,
    },
    primary_keys=("name", "timestamp"),
)
