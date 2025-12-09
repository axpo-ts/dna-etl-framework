"""Consumption measurement data for electricity supply points in the Spanish market."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import common_columns as standard_columns
from data_platform.data_model.metadata_common.tags_enum import DataDomain, DataSubdomain
from data_platform.data_model.silver.neuron import neuron_common
from data_platform.data_model.metadata_common.common_columns import standard_columns

neuron_standard_columns = neuron_common

measure_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="neuron", name="falcon_measure"),
    schema=StructType([
        neuron_standard_columns.DateFormattedColumn.to_struct_field(),
        neuron_standard_columns.DateColumn.to_struct_field(comment="Date of the measurement record."),
        neuron_standard_columns.Cups20Column.to_struct_field(comment="CUPS is a unique identifier for electricity supply points in Spain."),
        neuron_standard_columns.Cups22Column.to_struct_field(),
        StructField("line", StringType(), True, {"comment": "Line identifier or reference for the supply point."}),
        StructField("priority", StringType(), True, {"comment": "Origin of the data to be used as priority level of the measurement record."}),
        neuron_standard_columns.TypeColumn.to_struct_field(comment="Main classification of the measurement (e.g., active, reactive)."),
        neuron_standard_columns.SubtypeColumn.to_struct_field(comment="Detailed classification within the main measurement type."),
        standard_columns.UnitColumn.to_struct_field(comment="Unit of measurement for the value (e.g., kWh, MWh)."),
        neuron_standard_columns.FrequencyColumn.to_struct_field(),
        neuron_standard_columns.HOrderColumn.to_struct_field(comment="Hour/Quarter order or sequence within the day (0-23 or 0-95 for quarter-hours)."),
        neuron_standard_columns.HourColumn.to_struct_field(comment="Hour/Quarter component of the timestamp (as decimal, e.g., 1-24 or 1-96 for quarter-hours)."),
        neuron_standard_columns.TimestampCreationColumn.to_struct_field(),
        neuron_standard_columns.TimestampUpdateColumn.to_struct_field(comment="Timestamp when the record was last updated in the source system, Madrid/Europe."),
        neuron_standard_columns.ValueColumn.to_struct_field(comment="Measured or calculated value for the supply point and time period."),
        neuron_standard_columns.SrcCreatedByColumn.to_struct_field(),
        neuron_standard_columns.SrcCreatedAtColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
    ]),
    comment="Consumption measurement data for electricity supply points in the Spanish market.",
    sources=(TableIdentifier(catalog="bronze", schema="neuron", name="falcon_measure"),),
    tags={
        **neuron_common.NEURON_COMMON_TAGS,
        DataDomain.KEY: DataDomain.FUNDAMENTAL,
        DataSubdomain.KEY: DataSubdomain.CONSUMPTION,
    },
    primary_keys=("date", "cups22", "priority", "type", "subtype", "unit", "frequency", "h_order", "timestamp_update"),
)
