from __future__ import annotations

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.silver.bridge_tool.bridge_tool_common import BRIDGETOOL_COMMON_TAGS

from data_platform.data_model.metadata_common import standard_columns

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    IntegerType,
    BooleanType
)

tagetik_normal_accounts_model = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="bridge_tool", name="tagetik_fact_normal_accounts"),
    schema=StructType([
        StructField(name = "category_code_lelev_02", dataType=StringType()),
        StructField(name = "category_code_level_03", dataType=StringType()),
        StructField(name = "category_code_level_04", dataType=StringType()),
        StructField(name = "category_code_level_05", dataType=StringType()),
        StructField(name = "category_description", dataType=StringType()),
        StructField(name = "fst_account_code", dataType=StringType()),
        StructField(name = "fst_account_description", dataType=StringType()),
        StructField(name = "sort_key", dataType=StringType()),
        StructField(name = "dimension_2_segment_desc_level_04", dataType=StringType()),
        StructField(name = "dimension_2_sort_key_level_04", dataType=StringType()),
        StructField(name = "dimension_2_segment_code", dataType=StringType()),
        StructField(name = "entity_segment_desc_level_03", dataType=StringType()),
        StructField(name = "entity_segment_sort_key_level_03", dataType=StringType()),
        StructField(name = "entity_segment_desc_level_04", dataType=StringType()),
        StructField(name= "entity_segment_sort_key_level_04", dataType=StringType()),
        StructField(name = "entity_segment_desc_level_05", dataType=StringType()),
        StructField(name = "entity_segment_sort_key_level_05", dataType=StringType()),
        StructField(name = "entity_segment_code", dataType=StringType()),
        StructField(name = "scenario_version_filter", dataType=StringType()),
        StructField(name = "scenario_year", dataType=IntegerType()),
        StructField(name = "month_short", dataType=StringType()),
        StructField(name = "period", dataType=StringType()),
        StructField(name = "is_grand_total_row_total", dataType=BooleanType()),
        StructField(name = "value", dataType=StringType()),
        StructField(name = "category_key", dataType=StringType()),
        standard_columns.LicenseColumn.to_struct_field()
    ]),
    comment="Tagetik Normal accounts.",
    license="BT_DUMMY_TEST",
        tags={
        **BRIDGETOOL_COMMON_TAGS
    },

)
