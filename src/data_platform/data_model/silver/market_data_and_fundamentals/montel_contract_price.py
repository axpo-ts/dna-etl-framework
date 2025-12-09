from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

license = "DNA_MONTELNEWS_DATA_LICENSE"


montel_contract_price_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="montel_contract_price",
    ),
    schema=StructType(
        [
            StructField(
                "ticker_symbol",
                StringType(),
                False,
                {"comment": "The ticker symbol of the contract"},
            ),
            StructField(
                "contract_name",
                StringType(),
                True,
                {"comment": "The name of the contract"},
            ),
            StructField(
                "original_data_source",
                StringType(),
                True,
                {"comment": "The original data source of the contract data"},
            ),
            StructField(
                "market_code",
                StringType(),
                True,
                {"comment": "The market code where the contract is traded"},
            ),
            StructField(
                "generic_period",
                StringType(),
                True,
                {"comment": "The generic period of the contract"},
            ),
            StructField(
                "trading_date",
                DateType(),
                False,
                {"comment": "The trading date of the contract data"},
            ),
            StructField(
                "open",
                DoubleType(),
                True,
                {"comment": "The opening price of the contract"},
            ),
            StructField(
                "high",
                DoubleType(),
                True,
                {"comment": "The highest price of the contract during the trading session"},
            ),
            StructField(
                "low",
                DoubleType(),
                True,
                {"comment": "The lowest price of the contract during the trading session"},
            ),
            StructField(
                "close",
                DoubleType(),
                True,
                {"comment": "The closing price of the contract"},
            ),
            StructField(
                "settlement",
                DoubleType(),
                True,
                {"comment": "The settlement price of the contract"},
            ),
            StructField(
                "volume",
                DoubleType(),
                True,
                {"comment": "The trading volume of the contract"},
            ),
            StructField(
                "open_interest",
                DoubleType(),
                True,
                {"comment": "The open interest of the contract"},
            ),
            StructField(
                "delivery_start",
                DateType(),
                True,
                {"comment": "The start date of the delivery period"},
            ),
            StructField(
                "delivery_end",
                DateType(),
                True,
                {"comment": "The end date of the delivery period"},
            ),
            StructField(
                "contract_size",
                DoubleType(),
                True,
                {"comment": "The size of the contract"},
            ),
            StructField(
                "trading_start",
                DateType(),
                True,
                {"comment": "The start date of the trading period"},
            ),
            StructField(
                "trading_end",
                DateType(),
                True,
                {"comment": "The end date of the trading period"},
            ),
            StructField(
                "unit",
                StringType(),
                True,
                {"comment": "The unit of measurement for the contract"},
            ),
            StructField(
                "commodity",
                StringType(),
                True,
                {"comment": "The commodity type of the contract"},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
        ]
    ),
    comment="This table contains daily OHLCV and settlement data for a wide range of commodity contracts.",
    sources=[
        TableIdentifier(catalog="bronze", schema="montel", name="contract_prices"),
        TableIdentifier(catalog="bronze", schema="montel", name="contract_metadata"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "Montel",
    },
    primary_keys=("ticker_symbol", "trading_date"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
