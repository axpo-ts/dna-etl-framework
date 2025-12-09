# Databricks notebook source
from __future__ import annotations

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.reference.reference_common import REFERENCE_COMMON_TAGS, REFERENCE_LICENSE

reference_data_mapping_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="reference_data_mapping",
    ),
    schema=StructType(
        [
            # Core identification
            StructField("mapping_id", IntegerType(), False, {"comment": "Hash unique identifier for each mapping record"}),
            StructField("mapping_ref_group", StringType(), False, {"comment": "Logical grouping of related mappings (e.g., 'country_area', 'commodity_type')"}),
            StructField("mapping_ref_col", StringType(), False, {"comment": "Column name in the reference table that this mapping applies to"}),
            StructField("mapping_ref_value", StringType(), False, {"comment": "Standardized/canonical value used across the platform"}),
            # Simple mapping (most common case - when these are filled, mapping_type is automatically 'simple')
            StructField("source_column", StringType(), True, {"comment": "Column name for simple mappings (e.g., 'country')"}),
            StructField("source_value", StringType(), True, {"comment": "Value to match for simple mappings (e.g., 'France')"}),
            # Complex mapping
            StructField("match_expression", StringType(), True, {"comment": "SQL boolean expression for complex mappings (e.g., \"commodity_group = 'Green' AND commodity_type IN ('EUA', 'UKA')\")"}),
            StructField("mapping_type", StringType(), True, {"comment": "Simple or SQL expression mapping type (auto-derived)"}),
            StructField("description", StringType(), True, {"comment": "Business description of this mapping"}),
            # Mapping mode
            StructField("mode", StringType(), True, {"comment": "Mapping mode: 'replace' (modify source column) or 'enrich' (create new column). Defaults to 'replace' for backward compatibility"}),
            # Audit and validity
            standard_columns.IsCurrentColumn.to_struct_field(nullable=False, comment="Flag indicating if this is the current active mapping"),
            standard_columns.ValidFromColumn.to_struct_field(nullable=False, comment="Start date of mapping validity"),
            standard_columns.ValidToColumn.to_struct_field(comment="End date of mapping validity (NULL for current)"),
        ]
    ),
    comment="Reference data mapping table with automatic type detection based on filled columns",
    primary_keys=("mapping_id", "valid_from"),
    license=REFERENCE_LICENSE,
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.JAVIER_HERNANDEZ_MONTES,
    },
    audit_columns=("created_by", "updated_by"),
)
