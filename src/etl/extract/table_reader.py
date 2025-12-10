from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property

from pyspark.sql import DataFrame

from data_model import TableIdentifier
from etl.extract.base_reader import ReaderBase


@dataclass(kw_only=True)
class _BaseTableReader(ReaderBase):
    """Adds `table_model` support to the generic reader base."""

    table: TableIdentifier

    @property
    def full_table_name(self) -> str:
        """Get the full table name in Unity Catalog format (for this reader's table)."""
        if self._is_dna_medallion_catalog:
            return self._full_unitycatalog_name_of(self.table)
        else:
            # For non-medallion tables, we use the catalog.schema.table format
            return str(self.table)

    @cached_property
    def _is_dna_medallion_catalog(self) -> bool:
        ident: TableIdentifier = self.table
        return bool(
            ident.catalog
            and (
                "staging" in ident.catalog.lower()
                or "bronze" in ident.catalog.lower()
                or "silver" in ident.catalog.lower()
                or "gold" in ident.catalog.lower()
            )
        )

    # generic no-filter batch read
    def _load_whole_table(self) -> DataFrame:
        self.context.logger.debug(f"Loading table {self.full_table_name}")
        return self._reader(streaming=False).format(self.format_type).table(self.full_table_name)


@dataclass(kw_only=True)
class IncrementalStreamingTableReader(_BaseTableReader):
    """Reads a streaming table incrementally.

    Args:
       filter_options (Mapping[str, str]): Optional filters to apply to the read.
    """

    task_name: str = field(init=False, default="IncrementalStreamingTableReader")

    def execute(self) -> DataFrame:
        """Execute the incremental streaming table reader task."""
        kwargs: dict = {}

        df = self._reader(streaming=True).options(**kwargs).table(self.full_table_name)

        return df


@dataclass(kw_only=True)
class FullTableReader(_BaseTableReader):
    """Simplest possible batch read - no watermark, no filters."""

    task_name: str = field(init=False, default="FullTableReader")

    def execute(self) -> DataFrame:
        """Execute the table reader task."""
        return self._load_whole_table()
