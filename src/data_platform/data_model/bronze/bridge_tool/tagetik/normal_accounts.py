from __future__ import annotations

from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

tagetik_normal_accounts_model = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="bridge_tool", name="tagetik_fact_normal_accounts"),
    schema=StructType(
        [
            StructField(name="category_code_lelev_02", dataType=StringType()),
            StructField(name="category_code_level_03", dataType=StringType()),
            StructField(name="category_code_level_04", dataType=StringType()),
            StructField(name="category_code_level_05", dataType=StringType()),
            StructField(name="category_description", dataType=StringType()),
            StructField(name="fst_account_code", dataType=StringType()),
            StructField(name="fst_account_description", dataType=StringType()),
            StructField(name="sort_key", dataType=StringType()),
            StructField(name="category_key", dataType=StringType()),
            StructField(name="dimension_2_segment_desc_level_04", dataType=StringType()),
            StructField(name="dimension_2_sort_key_level_04", dataType=StringType()),
            StructField(name="dimension_2_segment_code", dataType=StringType()),
            StructField(name="entity_segment_desc_level_03", dataType=StringType()),
            StructField(name="entity_segment_sort_key_level_03", dataType=StringType()),
            StructField(name="entity_segment_desc_level_04", dataType=StringType()),
            StructField(name="entity_segment_sort_key_level_04", dataType=StringType()),
            StructField(name="entity_segment_desc_level_05", dataType=StringType()),
            StructField(name="entity_segment_sort_key_level_05", dataType=StringType()),
            StructField(name="entity_segment_code", dataType=StringType()),
            StructField(name="scenario_version_filter", dataType=StringType()),
            StructField(name="scenario_year", dataType=StringType()),
            StructField(name="month_short", dataType=StringType()),
            StructField(name="period", dataType=StringType()),
            StructField(name="is_grand_total_row_total", dataType=BooleanType()),
            StructField(name="ACT_vFST", dataType=DoubleType()),
            StructField(name="BUD_vFST", dataType=DoubleType()),
        ]
    ),
    comment="Tagetik Normal Accounts.",
)
