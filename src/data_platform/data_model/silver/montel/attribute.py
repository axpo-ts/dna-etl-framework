from __future__ import annotations

from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.montel.montel_common import MONTEL_COMMON_TAGS, montel_standard_columns

license = "DNA_MONTELNEWS_DATA_LICENSE"


montel_attribute_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="montel", name="attribute"),
    schema=StructType(
        [
            montel_standard_columns.ContractNameColumn.to_struct_field(comment="Plain-language name used in market screens (e.g. Dec-26)."),
            montel_standard_columns.ContractSizeColumn.to_struct_field(data_type=LongType(), comment="Standard quantity the contract covers (power: total MWh in the strip, gas/oil: number of lots, barrels, tonnes, etc.)."),
            montel_standard_columns.SymbolKeyColumn.to_struct_field(nullable=True, comment="Native symbol from the exchange or broker, Montel preserves the original string (optionally prefixed by SourceId)."),
            StructField("montel_symbol", StringType(), True, {"comment": "Unique absolute Montel identifier: SourceId + MarketCode + specific_period (e.g. EEX DEB MAR-2026)."}),
            StructField("front_code", StringType(), True, {"comment": "Relative period code that always points to next contract of that tenor (M1, Q2, Y3 ...)."}),
            StructField("front_symbol", StringType(), True, {"comment": "Relative Montel key constructed as SourceId + MarketCode + FrontCode (e.g. ICE BRN M1)."}),
            montel_standard_columns.MarketCodeColumn.to_struct_field(comment="Short code identifying the market segment inside the source (e.g. DEB, BRN)."),
            StructField("market_name", StringType(), True, {"comment": "Human-readable market name (German Power, Brent Crude Futures)."}),
            montel_standard_columns.ContractYearColumn.to_struct_field(),
            StructField("commodity_group", StringType(), True, {"comment": "Top-level asset class (Power, Gas, Oil, Coal, Green, Financial)"}),
            StructField("commodity_type", StringType(), True, {"comment": "Finer commodity category (e.g. Crude Oil, EUA, UK NG)"}),
            StructField("base_peak_type", StringType(), True, {"comment": "For power only: the load profile the contract represents (Base, Peak, OffPeak, SuperPeak)"}),
            standard_columns.DeliveryStartColumn.to_struct_field(data_type=StringType(), comment="timestamp when physical delivery begins."),
            standard_columns.DeliveryEndColumn.to_struct_field(data_type=StringType(), comment="timestamp when physical delivery finishes."),
            StructField("trading_start", StringType(), True, {"comment": "First timestamp (UTC) at which the instrument becomes tradable."}),
            StructField("trading_end", StringType(), True, {"comment": "Last timestamp (UTC) at which the instrument can be traded on its venue."}),
            standard_columns.DeliveryAreaColumn.to_struct_field(data_type=StringType(), comment="Geographic delivery zone or hub (GERMANY, NBP, ARA...)."),
            StructField("denomination", StringType(), True, {"comment": "Price denomination shown in quote fields, typically Currency / Unit (EUR/MWh, USD/bbl)."}),
            standard_columns.CurrencyColumn.to_struct_field(comment="Trading currency"),
            montel_standard_columns.GenericPeriodColumn.to_struct_field(comment="Period category (Year, Season, Quarter, Month, Week, Day, Hour)"),
            StructField("specific_period", StringType(), True, {"comment": "Exact delivery slice expressed in words or codes (Cal-26, MAR-26, W-12 2026)."}),
            StructField("liquidity", DoubleType(), True, {"comment": "Montel-computed score 0-1 based on quote and trade activity (>=0.7 = High, 0.5-0.7 = Medium, <0.5 = Low)."}),
            StructField("maximum_allowed_day_interval_size_for_trades", LongType(), True, {"comment": "Guard-rail for the /Trade/Get endpoint: the largest date range (in days) you may request in one call."}),
            montel_standard_columns.OtcColumn.to_struct_field(),
            StructField("spread", BooleanType(), True, {"comment": "Boolean flag, true if the contract is defined as the price difference between two legs."}),
            StructField("access_level", StructType([StructField("Access", BooleanType(), True), StructField("Delay", LongType(), True), StructField("HistoricAccess", StringType(), True)]), True, {"comment": "Entitlement to the instrument (realtime, delayed, no-access)"}),
            standard_columns.UnitColumn.to_struct_field(comment="Unit in which volume or quantity is measured (MWh, bbl, tCO2)."),
            montel_standard_columns.CommodityColumn.to_struct_field(comment="Underlying commodity name (often aligns with commodity_type but kept separately for cross-asset workflows)."),
            StructField("m_i_c", StringType(), True, {"comment": "ISO 10383 Market Identifier Code of the trading venue, when applicable."}),
            StructField("market", StringType(), True, {"comment": "Long descriptive name of the venue (kept for backward compatibility with older feeds)."}),
            standard_columns.LicenseColumn.to_struct_field(comment="Axpo internal license identifier"),
            standard_columns.DataSourceColumn.to_struct_field(comment="Three-letter ID of the contributing exchange, broker or data source (EEX, ICE, NDX ...)."),
            standard_columns.DataSystemColumn.to_struct_field(comment="The internal or external system, application, or platform we ingest data from"),
        ]
    ),
    comment="Attributes (e.g.,license, unit, etc.) that are associated with various entities within Montel.",
    sources=[TableIdentifier(catalog="bronze", schema="montel", name="contract_metadata")],
    license=license,
    tags={
        **MONTEL_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.YUFAN_HE,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
    },
    primary_keys=(),
    partition_cols=(),
    liquid_cluster_cols=(),
)
