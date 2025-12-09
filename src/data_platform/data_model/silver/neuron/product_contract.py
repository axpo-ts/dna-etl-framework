"""Product contract data for electricity supply points in the Spanish market (Neuron AA system)."""

from __future__ import annotations

from pyspark.sql.types import BooleanType, DecimalType, DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common.tags_enum import DataDomain
from data_platform.data_model.silver.neuron import neuron_common
from data_platform.data_model.metadata_common.common_columns import standard_columns

product_contract_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="neuron", name="falcon_product_contract"),
    schema=StructType([
        StructField("cups", StringType(), True, {"comment": "CUPS code identifier for the electricity supply point."}),
        StructField("id_contract", StringType(), True, {"comment": "Unique contract identifier."}),
        StructField("owner", StringType(), True, {"comment": "Owner of the product contract."}),
        StructField("account_id", StringType(), True, {"comment": "Account identifier associated with the contract."}),
        StructField("name", StringType(), True, {"comment": "Product contract name."}),
        StructField("root_asset", StringType(), True, {"comment": "Root asset identifier for the contract."}),
        StructField("contracted_power_p3_kw", DoubleType(), True, {"comment": "Contracted power for period P3 (kW)."}),
        StructField("annual_contracted_p1_kwh", DoubleType(), True, {"comment": "Annual contracted consumption for period P1 (kWh)."}),
        StructField("product_end_date", StringType(), True, {"comment": "Product end date."}),
        StructField("programming_unit", StringType(), True, {"comment": "Programming unit identifier."}),
        StructField("product_is_archived", BooleanType(), True, {"comment": "Flag indicating if the product is archived."}),
        StructField("marketer", StringType(), True, {"comment": "Marketer identifier."}),
        StructField("product_start_date", StringType(), True, {"comment": "Product start date."}),
        StructField("total_daily_consumption", DoubleType(), True, {"comment": "Total daily energy consumption."}),
        StructField("product_id", StringType(), True, {"comment": "Unique product identifier."}),
        StructField("is_competitor_product", BooleanType(), True, {"comment": "Flag indicating if this is a competitor's product."}),
        StructField("contract_edqa", DecimalType(38, 0), True, {"comment": "EDQA contract value."}),
        StructField("annual_contracted_p5_kwh", DoubleType(), True, {"comment": "Annual contracted consumption for period P5 (kWh)."}),
        StructField("annual_contracted_p3_kwh", DoubleType(), True, {"comment": "Annual contracted consumption for period P3 (kWh)."}),
        StructField("status", StringType(), True, {"comment": "Current status of the product contract."}),
        StructField("partner_margin", DoubleType(), True, {"comment": "Partner margin amount."}),
        StructField("annual_contracted_p6_kwh", DoubleType(), True, {"comment": "Annual contracted consumption for period P6 (kWh)."}),
        StructField("tax_id", StringType(), True, {"comment": "CIF tax identification number of the contract holder."}),
        StructField("toll_id", StringType(), True, {"comment": "Toll (peaje) identifier."}),
        StructField("axpo_margin", DoubleType(), True, {"comment": "Axpo margin amount."}),
        StructField("product_type", StringType(), True, {"comment": "Type of product (e.g., tariff, service)."}),
        StructField("commissioner", BooleanType(), True, {"comment": "Flag indicating if a commissioner is involved."}),
        StructField("cups_product", StringType(), True, {"comment": "CUPS product identifier."}),
        StructField("settlement_mode", StringType(), True, {"comment": "Settlement mode for the contract."}),
        StructField("base_price_id", StringType(), True, {"comment": "Base price identifier."}),
        StructField("qa", DoubleType(), True, {"comment": "Quality assurance value."}),
        StructField("contracted_power_p6_kw", DoubleType(), True, {"comment": "Contracted power for period P6 (kW)."}),
        StructField("contracted_product_code_aux", StringType(), True, {"comment": "Auxiliary contracted product code."}),
        StructField("product_business_line", StringType(), True, {"comment": "Internal business division responsible for the contract."}),
        StructField("contracted_power_p4_kw", DoubleType(), True, {"comment": "Contracted power for period P4 (kW)."}),
        StructField("contracted_power_p2_kw", DoubleType(), True, {"comment": "Contracted power for period P2 (kW)."}),
        StructField("asset_version", DecimalType(38, 0), True, {"comment": "Asset version number."}),
        StructField("product_type_classification", StringType(), True, {"comment": "Product type classification."}),
        StructField("contract", StringType(), True, {"comment": "Contract reference."}),
        StructField("annual_contracted_p2_kwh", DoubleType(), True, {"comment": "Annual contracted consumption for period P2 (kWh)."}),
        StructField("annual_contracted_p4_kwh", DoubleType(), True, {"comment": "Annual contracted consumption for period P4 (kWh)."}),
        neuron_common.SrcCreatedByColumn.to_struct_field(),
        neuron_common.SrcCreatedAtColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
    ]),
    comment="Product contract data for electricity supply points in the Spanish market (Neuron AA system).",
    sources=(TableIdentifier(catalog="bronze", schema="neuron", name="falcon_product_contract"),),
    primary_keys=("id_contract", "product_id"),
    tags={
        **neuron_common.NEURON_COMMON_TAGS,
        DataDomain.KEY: DataDomain.COUNTERPARTY,
    },
)
