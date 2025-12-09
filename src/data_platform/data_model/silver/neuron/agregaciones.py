"""Aggregated power consumption from the Iberia retail."""

from __future__ import annotations

from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common.tags_enum import DataDomain, DataSubdomain
from data_platform.data_model.silver.neuron import neuron_common
from data_platform.data_model.metadata_common.common_columns import standard_columns

neuron_standard_columns = neuron_common

agregaciones_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="neuron", name="falcon_aggregations"),
    schema=StructType([
        neuron_standard_columns.DateFormattedColumn.to_struct_field(comment="Date of the aggregation record formatted with hours and minutes, Madrid/Europe."),
        neuron_standard_columns.Cups20Column.to_struct_field(),
        neuron_standard_columns.HourColumn.to_struct_field(comment="Hour component of the timestamp"),
        neuron_standard_columns.HOrderColumn.to_struct_field(comment="Hour counter of the timestamp"),
        StructField("fare", StringType(), True, {"comment": "Fare type or classification of the contract"}),
        StructField("programming_unit", StringType(), True, {"comment": "Identifier of the group a client participates in the market"}),
        StructField("geographic_zone", StringType(), True, {"comment": "Geographic zone identifier"}),
        StructField("company_name", StringType(), True, {"comment": "Social denomination or company name"}),
        StructField("tax_id", StringType(), True, {"comment": "CIF/NIF tax identification number"}),
        StructField("province", StringType(), True, {"comment": "Province name"}),
        StructField("cnae", StringType(), True, {"comment": "CNAE (National Classification of Economic Activities) classification code"}),
        StructField("settled_active", DoubleType(), True, {"comment": "Active power consumption settled with the client. Original name: LIQUIDADA_ACTIVA."}),
        StructField("settled_loss", DoubleType(), True, {"comment": "Active power plus losses consumption settled with the client. Original name: LIQUIDADA_LOSS."}),
        StructField("best_measure_active", DoubleType(), True, {"comment": "Best measure for active power consumption settled with the client. Original name: MM_ACTIVA."}),
        StructField("best_measure_loss", DoubleType(), True, {"comment": "Best measure for active power plus losses consumption settled with the client. Original name: MM_LOSS."}),
        StructField("forecast_active", DoubleType(), True, {"comment": "Forecasted active power consumption"}),
        StructField("forecast_loss", DoubleType(), True, {"comment": "Forecasted active power plus losses consumption."}),
        StructField("forecast_original_active", DoubleType(), True, {"comment": "Original forecast for active power consumption."}),
        StructField("forecast_original_loss", DoubleType(), True, {"comment": "Original forecast for active power plus losses consumption."}),
        StructField("ree_active", DoubleType(), True, {"comment": "REE (TSO) active power consumption."}),
        neuron_standard_columns.DateColumn.to_struct_field(data_type=DateType(), comment="Date of the aggregation record"),
        neuron_standard_columns.SrcCreatedByColumn.to_struct_field(),
        neuron_standard_columns.SrcCreatedAtColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
    ]),
    comment="Aggregated power consumption from the Iberia retail.",
    sources=(TableIdentifier(catalog="bronze", schema="neuron", name="falcon_aggregations"),),
    tags={
        **neuron_common.NEURON_COMMON_TAGS,
        DataDomain.KEY: DataDomain.FUNDAMENTAL,
        DataSubdomain.KEY: DataSubdomain.CONSUMPTION,
    },
    primary_keys=("cups20", "date", "h_order", "tax_id", "src_created_at"),
)
