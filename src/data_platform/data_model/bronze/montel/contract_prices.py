from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

contract_prices_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "montel", "contract_prices"),
    schema=StructType(
        [
            StructField("SymbolKey", StringType(), True),
            StructField("ContractName", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Settlement", DoubleType(), True),
            StructField("Volume", DoubleType(), True),
            StructField("OpenInterest", DoubleType(), True),
        ]
    ),
    sources=None,
    primary_keys=(),
)
