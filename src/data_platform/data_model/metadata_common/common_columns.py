# Databricks notebook source
"""Standardized common column definitions for table metadata in the DNA Platform ETL.

This module defines individual column objects with predefined types and nullability.
Each column can be used directly in schema definitions with a simple .to_struct_field() call.

Example:
    >>> from data_platform.data_model.metadata_common.common_columns import (
    ...     DeliveryStartColumn,
    ...     UnitColumn,
    ...     CreatedAtColumn,
    ... )
    >>> from pyspark.sql.types import StructType
    >>>
    >>> # Simplified usage - just call to_struct_field()
    >>> schema = StructType(
    ...     [
    ...         DeliveryStartColumn.to_struct_field(),
    ...         UnitColumn.to_struct_field(),
    ...         CreatedAtColumn.to_struct_field(),
    ...     ]
    ... )
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    DataType,
    StringType,
    StructField,
    TimestampType,
)


@dataclass(frozen=True)
class StandardColumn:
    """A standard column with predefined name, comment, type, and nullability.

    Attributes:
        name: The column name.
        comment: The description/comment for the column.
        data_type: The PySpark data type for this column.
        nullable: Whether the column can contain null values.
    """

    name: str
    comment: str
    data_type: DataType
    nullable: bool = True

    @property
    def value(self) -> str:
        """Get the column name.

        Returns:
            The column name.
        """
        return self.name

    def to_struct_field(
        self,
        data_type: DataType | None = None,
        nullable: bool | None = None,
        comment: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StructField:
        """Create a StructField with this column definition.

        Args:
            data_type: Optional override for the data type. Uses default if not provided.
            nullable: Optional override for nullability. Uses default if not provided.
            comment: Optional override for the comment. Uses default if not provided.
            metadata: Additional metadata to merge with the comment.

        Returns:
            A StructField with the column definition.

        Example:
            >>> # Use defaults
            >>> field = DeliveryStartColumn.to_struct_field()
            >>> # Or override comment
            >>> field = DeliveryStartColumn.to_struct_field(comment="Custom comment")
            >>> # Or override nullable
            >>> field = DeliveryStartColumn.to_struct_field(nullable=True)
        """
        final_type = data_type if data_type is not None else self.data_type
        final_nullable = nullable if nullable is not None else self.nullable
        final_comment = comment if comment is not None else self.comment

        final_metadata = {"comment": final_comment}
        if metadata:
            final_metadata.update(metadata)

        return StructField(self.name, final_type, final_nullable, final_metadata)


# ============================================================================
# DELIVERY PERIOD COLUMNS
# ============================================================================

DeliveryStartColumn = StandardColumn(
    name="delivery_start",
    comment="The timestamp of the start of the delivery period in UTC",
    data_type=TimestampType(),
    nullable=False,
)

DeliveryEndColumn = StandardColumn(
    name="delivery_end",
    comment="The timestamp of the end of the delivery period in UTC",
    data_type=TimestampType(),
    nullable=False,
)

DurationColumn = StandardColumn(
    name="duration",
    comment="The time resolution in a standardized ISO 8601 duration format e.g. PT1H",
    data_type=StringType(),
    nullable=True,
)

DeliveryAreaColumn = StandardColumn(
    name="delivery_area",
    comment="The delivery area, e.g. BE",
    data_type=StringType(),
    nullable=False,
)


# ============================================================================
# MEASUREMENT COLUMNS
# ============================================================================

UnitColumn = StandardColumn(
    name="unit",
    comment="The unit of measurement associated with the value column",
    data_type=StringType(),
    nullable=False,
)

CommodityColumn = StandardColumn(
    name="commodity",
    comment="The type of energy or resource to which the data record relates",
    data_type=StringType(),
    nullable=False,
)

CurrencyColumn = StandardColumn(
    name="currency",
    comment="The monetary unit associated with the data record",
    data_type=StringType(),
    nullable=True,
)


# ============================================================================
# METADATA COLUMNS
# ============================================================================

LicenseColumn = StandardColumn(
    name="license",
    comment="The license package identifier (LPI) associated with the data",
    data_type=StringType(),
    nullable=False,
)

DataSourceColumn = StandardColumn(
    name="data_source",
    comment="The original provider or origin of the data",
    data_type=StringType(),
    nullable=False,
)

DataSystemColumn = StandardColumn(
    name="data_system",
    comment="The internal or external system, application, or platform we ingest data from",
    data_type=StringType(),
    nullable=False,
)

UpdatedAtSourceColumn = StandardColumn(
    name="updated_at_source",
    comment="Timestamp when the data was added to the SFTP export file. This column can be used to track delta changes in the files",
    data_type=TimestampType(),
    nullable=True,
)

# ============================================================================
# SCD TYPE 2 VALIDITY COLUMNS
# ============================================================================

ValidFromColumn = StandardColumn(
    name="valid_from",
    comment="The start timestamp from which the data is considered valid",
    data_type=TimestampType(),
    nullable=False,
)

ValidToColumn = StandardColumn(
    name="valid_to",
    comment="The end timestamp until which the data is considered valid",
    data_type=TimestampType(),
    nullable=True,
)

IsCurrentColumn = StandardColumn(
    name="is_current",
    comment="A boolean field indicating whether the data is currently valid",
    data_type=BooleanType(),
    nullable=False,
)


# ============================================================================
# AUDIT COLUMNS
# ============================================================================

CreatedAtColumn = StandardColumn(
    name="created_at",
    comment="The timestamp when the record was created in Databricks",
    data_type=TimestampType(),
    nullable=True,
)

CreatedByColumn = StandardColumn(
    name="created_by",
    comment="The user who created the record in Databricks",
    data_type=StringType(),
    nullable=True,
)

UpdatedAtColumn = StandardColumn(
    name="updated_at",
    comment="The timestamp when the record was last updated in Databricks",
    data_type=TimestampType(),
    nullable=True,
)

UpdatedByColumn = StandardColumn(
    name="updated_by",
    comment="The user who last updated the record in Databricks",
    data_type=StringType(),
    nullable=True,
)

TableIdColumn = StandardColumn(
    name="table_id",
    comment="A unique identifier for each record in the table",
    data_type=StringType(),
    nullable=True,
)


# ============================================================================
# REFERENCE COLUMNS
# ============================================================================

RefCommodityColumn = StandardColumn(
    name="_ref_commodity",
    comment="Standardized Level-0 commodity from reference data (e.g., POWER, GAS).",
    data_type=StringType(),
    nullable=True,
)


# ============================================================================
# CONVENIENCE ACCESS OBJECT
# ============================================================================


class StandardColumns:
    """Convenience accessor for all standard columns.

    Allows access via standard_columns.DeliveryStartColumn.to_struct_field()
    """

    DeliveryStartColumn = DeliveryStartColumn
    DeliveryEndColumn = DeliveryEndColumn
    DurationColumn = DurationColumn
    DeliveryAreaColumn = DeliveryAreaColumn

    UnitColumn = UnitColumn
    CommodityColumn = CommodityColumn
    CurrencyColumn = CurrencyColumn

    UpdatedAtSourceColumn = UpdatedAtSourceColumn
    LicenseColumn = LicenseColumn
    DataSourceColumn = DataSourceColumn
    DataSystemColumn = DataSystemColumn

    ValidFromColumn = ValidFromColumn
    ValidToColumn = ValidToColumn
    IsCurrentColumn = IsCurrentColumn

    RefCommodityColumn = RefCommodityColumn


# Singleton instance for convenient access
standard_columns = StandardColumns()
