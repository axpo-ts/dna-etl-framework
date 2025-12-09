from __future__ import annotations

from pyspark.sql.types import DateType, DoubleType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.montel.montel_common import MONTEL_COMMON_TAGS, montel_standard_columns

license = "DNA_MONTELNEWS_DATA_LICENSE"


price_volume_trade_statistic_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="montel", name="price_volume_trade_statistic"),
    schema=StructType(
        [
            montel_standard_columns.SymbolKeyColumn.to_struct_field(),
            montel_standard_columns.ContractNameColumn.to_struct_field(),
            montel_standard_columns.MarketCodeColumn.to_struct_field(),
            montel_standard_columns.ContractSizeColumn.to_struct_field(),
            montel_standard_columns.ContractYearColumn.to_struct_field(),
            montel_standard_columns.GenericPeriodColumn.to_struct_field(),
            StructField("reference_date", DateType(), False, {"comment": "The trading date of the contract data"}),
            standard_columns.DeliveryStartColumn.to_struct_field(data_type=DateType(), comment="The start date of the delivery period"),
            standard_columns.DeliveryEndColumn.to_struct_field(data_type=DateType(), comment="The end date of the delivery period"),
            StructField("open", DoubleType(), True, {"comment": "The opening price of the contract"}),
            StructField("high", DoubleType(), True, {"comment": "The highest price of the contract during the trading session"}),
            StructField("low", DoubleType(), True, {"comment": "The lowest price of the contract during the trading session"}),
            StructField("close", DoubleType(), True, {"comment": "The closing price of the contract"}),
            StructField("settlement", DoubleType(), True, {"comment": "The settlement price of the contract"}),
            StructField("volume", DoubleType(), True, {"comment": "The trading volume of the contract"}),
            StructField("open_interest", DoubleType(), True, {"comment": "The open interest of the contract"}),
            montel_standard_columns.UnitPriceColumn.to_struct_field(),
            montel_standard_columns.UnitVolumeColumn.to_struct_field(),
            montel_standard_columns.OtcColumn.to_struct_field(),
            montel_standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.RefCommodityColumn.to_struct_field(),
            montel_standard_columns.TradingStartColumn.to_struct_field(),
            montel_standard_columns.TradingEndColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(comment="Three-letter ID of the contributing exchange, broker or data source (EEX, ICE, NDX ...)."),
            standard_columns.DataSystemColumn.to_struct_field(comment="The internal or external system, application, or platform we ingest data from"),
        ]
    ),
    comment="This table contains daily OHLCV and settlement data for a wide range of commodity contracts.",
    sources=[
        TableIdentifier(catalog="bronze", schema="montel", name="contract_prices"),
        TableIdentifier(catalog="silver", schema="montel", name="attribute"),
    ],
    license=license,
    tags={
        **MONTEL_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.FORWARD,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.KNUT_STENSROD,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
    },
    primary_keys=("symbol_key", "reference_date"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
