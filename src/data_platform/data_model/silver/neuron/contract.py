"""Iberia retail contract data from Neuron AA system."""

from __future__ import annotations

from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common.tags_enum import DataDomain
from data_platform.data_model.silver.neuron import neuron_common
from data_platform.data_model.metadata_common.common_columns import standard_columns

contract_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="neuron", name="falcon_contract"),
    schema=StructType([
        StructField("id_contract", StringType(), True, {"comment": "Unique identifier for the contract."}),
        StructField("cups", StringType(), True, {"comment": "CUPS is a unique identifier for electricity supply points."}),
        StructField("atr", BooleanType(), True, {"comment": "ATR (Third-Party Access to the Grid) is a regulated fee and permission allowing customers to use the electricity network in Spain."}),
        StructField("account_name", StringType(), True, {"comment": "Name of the account holder or customer."}),
        StructField("activated_date", StringType(), True, {"comment": "Date when the contract was activated."}),
        StructField("advance", BooleanType(), True, {"comment": "Flag indicating if the contract includes advance payment."}),
        StructField("amount_fixed_fee", DoubleType(), True, {"comment": "Fixed fee amount associated with the contract."}),
        StructField("annual_volume", DoubleType(), True, {"comment": "Annual contracted energy volume (kWh)."}),
        StructField("annual_p1_kwh", DoubleType(), True, {"comment": "Annual energy consumption in period P1 (kWh)."}),
        StructField("annual_p2_kwh", DoubleType(), True, {"comment": "Annual energy consumption in period P2 (kWh)."}),
        StructField("annual_p3_kwh", DoubleType(), True, {"comment": "Annual energy consumption in period P3 (kWh)."}),
        StructField("bill_shipping_method", StringType(), True, {"comment": "Method used to send bills to the customer."}),
        StructField("business_division", StringType(), True, {"comment": "Internal business division responsible for the contract."}),
        StructField("business_line", StringType(), True, {"comment": "Business line or segment associated with the contract."}),
        StructField("crm_commission", DoubleType(), True, {"comment": "Commission amount for CRM channel."}),
        StructField("card_type", StringType(), True, {"comment": "Type of card used for contract or payment."}),
        StructField("cancel_reason", DoubleType(), True, {"comment": "Numeric code representing the reason for contract cancellation."}),
        StructField("dist_code", StringType(), True, {"comment": "Distribution company code associated with the supply point."}),
        StructField("agent_commission", DoubleType(), True, {"comment": "Commission amount for the sales agent."}),
        StructField("boss_commission", DoubleType(), True, {"comment": "Commission amount for the supervising manager."}),
        StructField("subagent_commission", DoubleType(), True, {"comment": "Commission amount for the sub-agent."}),
        StructField("contract_code", StringType(), True, {"comment": "Primary code identifying the contract."}),
        StructField("contract_code_2", StringType(), True, {"comment": "Secondary code identifying the contract."}),
        StructField("contract_code_final", StringType(), True, {"comment": "Final or definitive code for the contract."}),
        StructField("counter_party", StringType(), True, {"comment": "Identifier for the counterparty in the contract."}),
        StructField("customer_signed_date", StringType(), True, {"comment": "Date when the customer signed the contract."}),
        StructField("country", StringType(), True, {"comment": "Country code for the contract location."}),
        StructField("tariff", StringType(), True, {"comment": "Tariff type or rate applied to the contract."}),
        StructField("end_contract", DoubleType(), True, {"comment": "Indicator or code marking contract end."}),
        StructField("estimated_start_date_contract", StringType(), True, {"comment": "Estimated contract start date."}),
        StructField("pricing_date", StringType(), True, {"comment": "Date when the PRICING team started processing the client."}),
        StructField("green_product", BooleanType(), True, {"comment": "Flag indicating if the contract is for a green/renewable energy product."}),
        StructField("termination_reason", DoubleType(), True, {"comment": "Numeric code for contract termination reason."}),
        StructField("product_name", StringType(), True, {"comment": "Name of the contracted energy product."}),
        StructField("start_date", StringType(), True, {"comment": "Actual start date of the contract."}),
        StructField("state", StringType(), True, {"comment": "Municipality or region where the contract is registered."}),
        StructField("status", StringType(), True, {"comment": "Current status of the contract (scratch, active, terminated...)."}),
        StructField("cups22", StringType(), True, {"comment": "Combination of the cups identifier plus the line code."}),
        StructField("timestamp_creation", TimestampType(), True, {"comment": "Timestamp when the record was created in the source system, Madrid/Europe."}),
        StructField("timestamp_update", TimestampType(), True, {"comment": "Timestamp when the record was last updated in the source system, Madrid/Europe."}),
        StructField("p4_annual_kwh", DoubleType(), True, {"comment": "Annual energy consumption in period P4 (kWh)."}),
        StructField("p5_annual_kwh", DoubleType(), True, {"comment": "Annual energy consumption in period P5 (kWh)."}),
        StructField("p6_annual_kwh", DoubleType(), True, {"comment": "Annual energy consumption in period P6 (kWh)."}),
        StructField("created_date", StringType(), True, {"comment": "Date when the record was created in the original system."}),
        StructField("theoretical_contract_end_date", TimestampType(), True, {"comment": "Theoretical contract end date."}),
        StructField("definitive_contract_end_date", TimestampType(), True, {"comment": "Definitive contract end date."}),
        neuron_common.SrcCreatedByColumn.to_struct_field(),
        neuron_common.SrcCreatedAtColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
    ]),
    comment="Iberia retail contract data from Neuron AA system.",
    sources=(TableIdentifier(catalog="bronze", schema="neuron", name="falcon_contract"),),
    primary_keys=("id_contract",),
    tags={
        **neuron_common.NEURON_COMMON_TAGS,
        DataDomain.KEY: DataDomain.COUNTERPARTY,
    },
)
