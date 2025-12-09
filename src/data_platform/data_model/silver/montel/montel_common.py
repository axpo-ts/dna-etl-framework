"""Common column definitions and tags for Montel schema tables."""

from __future__ import annotations

from pyspark.sql.types import BooleanType, DateType, DoubleType, StringType

from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.metadata_common.common_columns import StandardColumn

# Common tags for Montel tables (shared across all three tables)
MONTEL_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
}


# Columns used in more than one table
SymbolKeyColumn = StandardColumn(
    name="symbol_key",
    comment="The ticker symbol of the contract",
    data_type=StringType(),
    nullable=False,
)

ContractNameColumn = StandardColumn(
    name="contract_name",
    comment="The name of the contract",
    data_type=StringType(),
    nullable=True,
)

MarketCodeColumn = StandardColumn(
    name="market_code",
    comment="The market code where the contract is traded",
    data_type=StringType(),
    nullable=True,
)

ContractSizeColumn = StandardColumn(
    name="contract_size",
    comment="The size of the contract",
    data_type=DoubleType(),
    nullable=True,
)

ContractYearColumn = StandardColumn(
    name="contract_year",
    comment="Calendar year of the delivery period (four digits).",
    data_type=StringType(),
    nullable=True,
)

GenericPeriodColumn = StandardColumn(
    name="generic_period",
    comment="The generic period of the contract",
    data_type=StringType(),
    nullable=True,
)

OtcColumn = StandardColumn(
    name="otc",
    comment="true if an over-the-counter variant of the contract (broker quotes) is available.",
    data_type=BooleanType(),
    nullable=True,
)

CommodityColumn = StandardColumn(
    name="commodity",
    comment="The commodity type of the contract",
    data_type=StringType(),
    nullable=True,
)

# Additional columns used in 2+ tables
UnitPriceColumn = StandardColumn(
    name="unit_price",
    comment="The unit in which the price is denominated",
    data_type=StringType(),
    nullable=True,
)

UnitVolumeColumn = StandardColumn(
    name="unit_volume",
    comment="The unit volume in which the volume is measured",
    data_type=StringType(),
    nullable=True,
)

TradingStartColumn = StandardColumn(
    name="trading_start",
    comment="The start date of the trading period",
    data_type=DateType(),
    nullable=True,
)

TradingEndColumn = StandardColumn(
    name="trading_end",
    comment="The end date of the trading period",
    data_type=DateType(),
    nullable=True,
)


class MontelStandardColumns:
    """Accessor class for Montel standard columns."""

    SymbolKeyColumn = SymbolKeyColumn
    ContractNameColumn = ContractNameColumn
    MarketCodeColumn = MarketCodeColumn
    ContractSizeColumn = ContractSizeColumn
    ContractYearColumn = ContractYearColumn
    GenericPeriodColumn = GenericPeriodColumn
    OtcColumn = OtcColumn
    CommodityColumn = CommodityColumn
    UnitPriceColumn = UnitPriceColumn
    UnitVolumeColumn = UnitVolumeColumn
    TradingStartColumn = TradingStartColumn
    TradingEndColumn = TradingEndColumn


montel_standard_columns = MontelStandardColumns()
