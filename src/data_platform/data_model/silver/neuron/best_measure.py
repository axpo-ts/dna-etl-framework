"""Best measurement data from silver layer (best available values for each point in time)."""

from __future__ import annotations

from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import common_columns as standard_columns
from data_platform.data_model.metadata_common.tags_enum import DataDomain, DataSubdomain
from data_platform.data_model.silver.neuron import neuron_common
from data_platform.data_model.metadata_common.common_columns import standard_columns

neuron_standard_columns = neuron_common

best_measure_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="neuron", name="falcon_best_measure"),
    schema=StructType([
        neuron_standard_columns.DateFormattedColumn.to_struct_field(),
        neuron_standard_columns.DateColumn.to_struct_field(comment="Timestamp of the measurement record."),
        neuron_standard_columns.Cups20Column.to_struct_field(),
        neuron_standard_columns.Cups22Column.to_struct_field(comment="Combination of the CUPS20 with the 2-digit line code."),
        StructField("source", StringType(), True, {"comment": "Source of the measurement data"}),
        neuron_standard_columns.TypeColumn.to_struct_field(),
        neuron_standard_columns.SubtypeColumn.to_struct_field(comment="Detailed classification within the main type."),
        standard_columns.UnitColumn.to_struct_field(comment="Unit of measurement (e.g., kWh, MW)."),
        neuron_standard_columns.FrequencyColumn.to_struct_field(),
        StructField("market", StringType(), True, {"comment": "Market identifier or classification."}),
        neuron_standard_columns.ValueColumn.to_struct_field(),
        StructField("value_source", StringType(), True, {"comment": "Origin or system providing the measurement data."}),
        StructField("time_period", DecimalType(38, 0), True, {"comment": "Time period identifier (e.g., hour of day, quarter of the day)."}),
        StructField("time_period_order", DecimalType(38, 0), True, {"comment": "Order of the time period within the day or interval."}),
        neuron_standard_columns.TimestampUpdateColumn.to_struct_field(comment="Timestamp when the record was last updated in the original system, Madrid/Europe."),
        neuron_standard_columns.TimestampCreationColumn.to_struct_field(comment="Timestamp when the record was originally created in the original system, Madrid/Europe."),
        neuron_standard_columns.SrcCreatedByColumn.to_struct_field(),
        neuron_standard_columns.SrcCreatedAtColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
    ]),
    comment="Best measurement data from silver layer (best available values for each point in time).",
    sources=(TableIdentifier(catalog="bronze", schema="neuron", name="falcon_best_measure"),),
    tags={
        **neuron_common.NEURON_COMMON_TAGS,
        DataDomain.KEY: DataDomain.FUNDAMENTAL,
        DataSubdomain.KEY: DataSubdomain.CONSUMPTION,
    },
    primary_keys=("date", "cups22", "type", "subtype", "unit", "frequency", "value_source", "time_period_order", "timestamp_update"),
)
