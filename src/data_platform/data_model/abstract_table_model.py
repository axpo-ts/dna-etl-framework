from __future__ import annotations

import json
from abc import abstractmethod
from collections.abc import Sequence

from pyspark.sql.types import StructType

from data_platform.data_model.abstract_unity_catalog_data_object_reference import (
    AbstractUnityCatalogDataObjectReference,
)
from data_platform.data_model.file_volume_identifier import FileVolumeIdentifier
from data_platform.data_model.table_identifier import TableIdentifier


class AbstractTableModel(AbstractUnityCatalogDataObjectReference):
    """Shared contract for a data-platform table definition.

    Sub-classes *must* supply:
        • the identifier (catalog / schema / table)
        • the column schema (`StructType`)

    Everything else is optional and therefore provided with defaults.
    """

    _BASE_AUDIT_COLS: tuple[str, ...] = (
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
    )

    @property
    @abstractmethod
    def schema(self) -> StructType:
        """Spark StructType describing all data columns."""
        ...

    @property
    @abstractmethod
    def audit_columns(self) -> Sequence[str]:
        """Tuple of audit columns to ignore in change detection.

        Logic:
        • Always include the four base columns.
        • If the table lives in the *silver* or *gold* catalog layer,
          automatically append ``table_id``.

        Returns:
            Immutable tuple of column names.
        """
        ...

    @property
    def constraints(self) -> dict[str, str]:
        """Returns a dictionary of constraints for the table."""
        return {}

    @property
    def sources(self) -> Sequence[TableIdentifier, FileVolumeIdentifier] | None:
        """Returns a sequence of sources objects for the table."""
        return None

    @property
    def partition_cols(self) -> Sequence[str]:
        """Returns a sequence of partition column names."""
        return ()

    @property
    def primary_keys(self) -> Sequence[str]:
        """Returns a sequence of primary key column names."""
        return ()

    @property
    def liquid_cluster_cols(self) -> Sequence[Sequence[str]]:
        """Returns a sequence of sequences of column names for liquid clustering."""
        return ()

    @property
    def schema_ddl(self) -> str:
        """Spark-compatible DDL string for the table columns."""
        if self.schema:
            return ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in self.schema.fields)

    def writer_options(self) -> dict[str, str]:
        """Returns a dict ready to feed into your CreateTable."""
        opts: dict[str, str] = {}
        if self.partition_cols:
            opts["partition_cols"] = ", ".join(self.partition_cols)
        if self.primary_keys:
            opts["primary_keys"] = ", ".join(self.primary_keys)
        return opts

    def as_dict(self) -> dict:
        """Serialise the definition handy for catalog UIs, audits, etc."""
        return {
            "name": self.full_name,
            "comment": self.comment,
            "schema_json": json.dumps(self.columns.jsonValue()),  # exact Spark schema
            "partition_cols": list(self.partition_cols),
            "primary_keys": list(self.primary_keys),
            "liquid_clusters": [list(c) for c in self.liquid_cluster_cols],
            "sources": list(self.sources or []),
        }
