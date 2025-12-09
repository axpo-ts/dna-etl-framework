from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

tagetik_fx_rates_model = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="bridge_tool", name="tagetik_fact_fx_rates"),
    schema=StructType(
        [
            StructField(name="currency_to_code", dataType=StringType()),
            StructField(name="fx_rate", dataType=DoubleType()),
            StructField(name="period_code", dataType=StringType()),
            StructField(name="scenario_code", dataType=StringType()),
            StructField(name="type", dataType=StringType()),
        ]
    ),
    comment="Tagetik Fx rates.",
)
