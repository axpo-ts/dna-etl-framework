from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cached_property

from pyspark.sql import DataFrame

from data_platform.data_model import TableIdentifier
from data_platform.etl.extract.base_reader import ReaderBase
from data_platform.tasks.reader.utils import (
    get_filter_conditions,
    get_watermark_filter,
)


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
class IncrementalTableReader(_BaseTableReader):
    """Reads a table incrementally, applying watermark and filter conditions.

    Args:
       filter_options (Mapping[str, str]): Optional filters to apply to the read.
       watermark_days (int | None): Optional number of days for buffer on update_col filtering.
       watermark_filter_col (str | None): Column to apply watermark filtering on.
       filter_updated_at_table (Mapping[str, str] | None): Optional table name to use for updated_at filtering.
        The first value is the target table's catalog.schema.table name.
        The second value is the id column name to join on (optional for getting the cutoff_date at id level).
        The table should have an 'updated_at' column to compare against.
    """

    filter_options: Mapping[str, str] = field(default_factory=dict)
    task_name: str = field(init=False, default="IncrementalTableReader")
    watermark_days: int | None = None
    watermark_filter_col: str | None = None
    filter_updated_at_table: Mapping[str, str] | None = None

    def execute(self) -> DataFrame:
        """Execute the incremental table reader task."""
        log = self.context.logger
        preds: list[str] = []

        if self.filter_updated_at_table:
            full_table_filter_updated_at = self._full_unitycatalog_name_of(
                self.filter_updated_at_table.get("table_name")
            )
            id_column_name = self.filter_updated_at_table.get("id_column_name", None)

            query = f"""
                s.updated_at >= (
                    select coalesce(
                        max(updated_at),
                        to_timestamp('1999-01-01 00:00:00')
                    ) as cutoff_date
                    from {full_table_filter_updated_at} as t
                """
            if id_column_name is None:
                query += ")"
            else:
                query += f"""
                    where t.{id_column_name} is not null and t.{id_column_name} = s.{id_column_name}
                )
                """

            preds.append(query)
            log.info(f"Applied updated_at > from {full_table_filter_updated_at}")

        if self.watermark_days is not None and self.watermark_filter_col:
            wm = get_watermark_filter(
                self.context,
                self.full_table_name,
                self.watermark_filter_col,
                self.watermark_days,
            )
            if wm:
                preds.append(wm)
                log.info(f"Applied watermark: {wm}")

        if self.filter_options:
            extra = get_filter_conditions(self.context, self.filter_options)
            preds.extend(extra)
            log.info(f"Applied custom filters: {extra}")

        query = f"SELECT * FROM {self.full_table_name} as s"
        if preds:
            query += " WHERE " + " AND ".join(preds)

        log.info(f"Executing query:\n{query}")
        df = self.context.spark.sql(query)

        return df


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
