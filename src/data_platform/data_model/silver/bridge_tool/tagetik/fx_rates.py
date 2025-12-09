from __future__ import annotations

from pyspark.sql.types import FloatType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.silver.bridge_tool.bridge_tool_common import BRIDGETOOL_COMMON_TAGS
from data_platform.data_model.metadata_common import standard_columns

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

tagetik_fx_rates_model = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="bridge_tool", name="tagetik_fact_fx_rates"),
    schema=StructType([
        StructField(name = "currency_to_code", dataType=StringType()),
        StructField(name = "fx_rate", dataType=DoubleType()),
        StructField(name = "period_code", dataType=StringType()),
        StructField(name = "scenario_code", dataType=StringType()),
        StructField(name = "type", dataType=StringType()),
        StructField(name = "plan_type", dataType=StringType()),
        StructField(name = "custom_fiscal_period", dataType=StringType()),
        StructField(name = "custom_period", dataType=StringType()),
        standard_columns.LicenseColumn.to_struct_field()
    ]),
    comment="Tagetik Fx rates.",
    license="BT_DUMMY_TEST",
    primary_keys=["currency_to_code", "scenario_code", "period_code", "type"],
        tags={
        **BRIDGETOOL_COMMON_TAGS
    },

)
