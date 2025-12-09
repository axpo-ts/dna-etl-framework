from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

import pyspark.sql.functions as f
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from data_platform.data_model import AbstractTableModel, FileVolumeIdentifier, TableIdentifier
from data_platform.etl.core import TaskContext
from data_platform.etl.etl_task import ETLTask
from data_platform.tasks.writer.utils import WriterUtilities


@dataclass(kw_only=True)
class _BaseTableWriter(ETLTask):
    """Helper for writing to Delta tables without MERGE (append, overwrite, batch-upsert, streaming-upsert).

    Supported patterns
    ------------------
    * **Append** - insert rows as-is.
    * **Overwrite** - drop duplicate PK rows (latest-wins) then overwrite the table.
    * **Batch upsert** - run a single MERGE against a static DataFrame.
    * **Streaming upsert** - run a MERGE for every micro-batch emitted by a streaming query.

    Parameters
    ----------
    context :
        ``TaskContext`` exposing Spark session, logger, metrics, etc.
    table_model :
        ``AbstractTableModel`` describing the target Delta table.
    source_df :
        Input ``pyspark.sql.DataFrame`` to be written.
    primary_keys :
        One or more column names that uniquely identify a row; accepts a
        sequence or a comma-separated string.
    writer_options :
        Extra options forwarded to the underlying ``DataFrameWriter`` (for
        example, ``{"checkpointLocation": "/mnt/..."}``).
    """

    context: TaskContext
    table_model: AbstractTableModel
    source_df: DataFrame | None = None
    primary_keys: Sequence[str] | str = field(default_factory=list)
    writer_options: Mapping[str, str] = field(default_factory=dict)
    drop_duplicates: bool = False  # whether to deduplicate the incoming data on primary keys
    order_by_col: str = "updated_at"
    created_column: str = "created_at"

    def __post_init__(self) -> None:
        if not self.primary_keys:
            self.primary_keys = self._to_list(self.table_model.primary_keys)

    # ---------------- internal helpers --------------------------------
    @staticmethod
    def _to_list(v: Sequence[str] | str) -> list[str]:
        return v if isinstance(v, list | tuple) else [c.strip() for c in v.split(",")]

    @property
    def _full_table_name(self) -> str:
        return self._full_unitycatalog_name_of(self.table_model.identifier)

    def _apply_dedup(self, df: DataFrame) -> DataFrame:
        """Return df as-is or deduplicated depending on self.drop_duplicates.

        If self.drop_duplicates is True and self.order_by_col is present in df,
        perform latest-wins dedup per primary key using a window with row_number().
        Otherwise, fall back to dropDuplicates(primary_keys).

        Args:
            df (DataFrame): Input DataFrame to potentially deduplicate.

        Returns:
            DataFrame: Deduplicated DataFrame if drop_duplicates is True; otherwise original df.
        """
        if not self.drop_duplicates:
            return df  # fast path
        pk = self._to_list(self.primary_keys)
        return apply_dedup(df=df, primary_keys=pk, order_by_col=self.order_by_col)

    def _stream(
        self,
        df: DataFrame,
        *,
        output_mode: str,  # "append" | "complete" | "update"
        foreach_fn: Callable[[DataFrame, int], None] | None = None,
    ) -> None:
        """Run a writeStream with all boiler-plate handled once:.

        • checkpoint location defaulting
        • trigger / writer options re-used across tasks
        """
        if foreach_fn is None and not df.isStreaming and self.drop_duplicates:
            df = self._apply_dedup(df)

        # this is to accomodate the case when the streaming source is a table already, not a volume
        if not isinstance(self.table_model.sources[0], TableIdentifier):
            if self.streaming_source and "checkpointLocation" in self.writer_options:
                base = self._full_base_source_filename_of(self.streaming_source)
                self.writer_options["checkpointLocation"] = f"{base}/{self.writer_options['checkpointLocation']}"

        writer = (
            df.writeStream.format("delta")
            .outputMode(output_mode)
            .options(**self.writer_options)
            .trigger(**self.trigger_kwargs)
        )

        if foreach_fn:  # upsert / merge path
            writer.foreachBatch(foreach_fn).start().awaitTermination()
        else:  # pure append / overwrite
            writer.toTable(self._full_table_name).awaitTermination()

    def _set_source_df(self, source_df: DataFrame) -> None:
        """Set the source DataFrame for the writer task."""
        if source_df is not None:
            self.source_df = source_df

    @abstractmethod
    def execute(self, source_df: DataFrame | None = None) -> None:
        """Execute the table write operation.

        Parameters
        ----------
        source_df : DataFrame or None, optional
            The source DataFrame to be written.
            If provided, this DataFrame should be used as the input for the write operation.
            If not provided, subclasses may use the `self.source_df` attribute if it has been set previously.

        Notes:
        -----
        Subclasses should document whether they require `source_df` to be passed explicitly,
         or if they rely on the class attribute.
        If both are present, subclasses should define which takes precedence.
        """
        raise NotImplementedError("Subclasses must implement the execute method to perform the write operation.")


@dataclass
class BatchTableOverwrite(_BaseTableWriter):
    """Overwrites the target Delta table with the incoming DataFrame."""

    task_name: str = field(init=False, default="BatchTableOverwrite")
    enable_schema_evolution: bool = False
    include_metadata: bool = False
    overwrite_schema: bool = False  # whether to overwrite the schema in the table

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Perform a batch overwrite operation on the target Delta table."""
        self._set_source_df(source_df)
        self.context.logger.info(f"Overwriting {self._full_table_name}")

        if self.overwrite_schema:
            self.writer_options["overwriteSchema"] = "true"
        else:
            self.writer_options["overwriteSchema"] = "false"

        # Add schema evolution support
        if self.enable_schema_evolution:
            self.writer_options["mergeSchema"] = "true"
            self.context.logger.info("Schema evolution enabled for overwrite")

        source_df = self.source_df

        if self.include_metadata and "_source_file" not in source_df.columns:
            source_df = source_df.withColumn("_source_file", f.col("_metadata.file_name"))
            self.context.logger.info("Added _source_file column")

        cleaned = self._apply_dedup(source_df)
        cleaned.write.format("delta").mode("overwrite").options(**self.writer_options).saveAsTable(
            self._full_table_name
        )


@dataclass
class BatchTableInsertOverwrite(_BaseTableWriter):
    """Insert Overwrites the target Delta table with the incoming DataFrame.

    Columns need to be in order in the dataframe
    """

    task_name: str = field(init=False, default="BatchTableInsertOverwrite")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Perform a batch overwrite operation on the target Delta table."""
        self._set_source_df(source_df)
        self.context.logger.info(f"Insert Overwriting {self._full_table_name}")

        cleaned = self._apply_dedup(self.source_df)
        cleaned.createOrReplaceTempView("temp_table")
        columns = cleaned.columns
        columns_str = ", ".join(columns)

        self.context.spark.sql(f"""
            INSERT OVERWRITE TABLE {self._full_table_name} ({columns_str})
            SELECT {columns_str} FROM temp_table
        """)


@dataclass
class BatchTableAppend(_BaseTableWriter):
    """Appends the DataFrame to the target Delta table."""

    task_name: str = field(init=False, default="BatchTableAppend")
    enable_schema_evolution: bool = False
    include_metadata: bool = False

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Append the DataFrame to the target Delta table."""
        self._set_source_df(source_df)
        self.context.logger.info(f"Appending to {self._full_table_name}")

        # Add schema evolution support
        if self.enable_schema_evolution:
            self.writer_options["mergeSchema"] = "true"
            self.context.logger.info("Schema evolution enabled for append")

        source_df = self.source_df

        # f.input_file_name() is deprecated, see page:
        # https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/input_file_name
        if self.include_metadata and "_source_file" not in source_df.columns:
            # https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-metadata-column
            source_df = source_df.withColumn("_source_file", f.col("_metadata.file_name"))
            self.context.logger.info("Added _source_file column")

        cleaned = self._apply_dedup(source_df)
        (cleaned.write.format("delta").mode("append").options(**self.writer_options).saveAsTable(self._full_table_name))


@dataclass
class StreamingTableComplete(_BaseTableWriter):
    """Replaces the **entire** table on every micro-batch after latest-wins de-duplication.

    Use only when each micro-batch already contains the full snapshot
    (e.g., small dimension tables or “available-now” streams).
    """

    source_df: DataFrame
    trigger_kwargs: Mapping[str, str] = field(default_factory=dict)
    streaming_source: FileVolumeIdentifier | None = None
    task_name: str = field(init=False, default="StreamingTableComplete")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Run the streaming complete operation, replacing the target table with each micro-batch."""
        self._set_source_df(source_df)
        self.context.logger.info(f"Streaming complete → {self._full_table_name}")

        self.writer_options.setdefault("truncate", "true")

        self._stream(
            self.source_df,
            output_mode="complete",  # Spark overwrites the target
        )


@dataclass
class StreamingTableAppend(_BaseTableWriter):
    """Continuously appends incoming micro-batches to the target Delta table."""

    source_df: DataFrame
    trigger_kwargs: Mapping[str, str] = field(default_factory=dict)
    streaming_source: FileVolumeIdentifier | None = None
    task_name: str = field(init=False, default="StreamingTableAppend")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Run the streaming append operation, continuously adding data to the target table."""
        self._set_source_df(source_df)
        self.context.logger.info(f"Streaming append  → {self._full_table_name}")

        self._stream(
            self.source_df,
            output_mode="append",
        )


@dataclass
class StreamingTableUpdate(_BaseTableWriter):
    """Continuously updates the target table with the incoming streaming data."""

    source_df: DataFrame
    trigger_kwargs: Mapping[str, str] = field(default_factory=dict)
    streaming_source: FileVolumeIdentifier | None = None
    task_name: str = field(init=False, default="StreamingTableUpdate")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Run the streaming update operation, continuously updating the target table."""
        self._set_source_df(source_df)
        self.context.logger.info(f"Streaming update → {self._full_table_name}")

        self._stream(
            self.source_df,
            output_mode="update",
        )


# ────────────────────────────────────────────────────────────────────────
#                 shared merge logic (now extends _BaseTableWriter)
# ────────────────────────────────────────────────────────────────────────
@dataclass(kw_only=True)
class _BaseTableUpsert(_BaseTableWriter):
    updated_columns: Sequence[str] | str = ("updated_at", "updated_by")
    created_columns: Sequence[str] | str = ("created_at", "created_by")
    ignore_columns: Sequence[str] | str = ("table_id", "_source_file")
    smart_update: bool = True  # whether to only update changed rows
    enable_schema_evolution: bool = False  # Add this to base class

    # ---- normalise the sequence-or-string fields ---------------------
    def __post_init__(self) -> None:
        self.created_columns = self._to_list(self.created_columns)
        self.updated_columns = self._to_list(self.updated_columns)
        self.ignore_columns = self._to_list(self.ignore_columns)

    # ---- core MERGE ---------------------------------------------------

    def _run_merge(self, source_df: DataFrame) -> None:
        # keep your dedup logic
        src = self._apply_dedup(source_df)

        merge_builder = generate_merge(
            primary_keys=self._to_list(self.primary_keys),
            target_table_name=self._full_table_name,
            input_df=src,
            smart_update=self.smart_update,
            ignore_columns=self.ignore_columns,
            schema_evolution=self.enable_schema_evolution,
            created_columns=self._to_list(self.created_columns),
            updated_columns=self._to_list(self.updated_columns),
        )
        merge_builder.execute()


# ────────────────────────────────────────────────────────────────────────
#                               Batch variant
# ────────────────────────────────────────────────────────────────────────
@dataclass
class BatchTableUpsert(_BaseTableUpsert):
    """Batch upsert operation that merges the source DataFrame into the target Delta table."""

    source_df: DataFrame
    task_name: str = field(init=False, default="BatchTableUpsert")
    include_metadata: bool = False

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Run the batch upsert operation, merging the source DataFrame into the target Delta table."""
        self._set_source_df(source_df)
        source_df = self.source_df

        if self.include_metadata and "_source_file" not in source_df.columns:
            source_df = source_df.withColumn("_source_file", f.col("_metadata.file_name"))
            self.context.logger.info("Added _source_file column")

        self._run_merge(source_df)
        self.context.logger.info(f"Batch upsert → {self._full_table_name}")


# ────────────────────────────────────────────────────────────────────────
#                         Base Streaming Upsert Logic
# ────────────────────────────────────────────────────────────────────────
@dataclass
class _BaseStreamingTableUpsert:
    """Base class for streaming table upsert operations with serialization-safe logic."""

    def _create_standalone_foreach_batch(
        self,
        table_name: str,
        primary_keys: list[str],
        created_columns: list[str],
        updated_columns: list[str],
        ignore_columns: list[str],
        enable_schema_evolution: bool,
        smart_update: bool,
        order_by_col: str,
    ) -> Callable[[DataFrame, int], None]:
        def _foreach_batch_standalone(micro_df: DataFrame, batch_id: int) -> None:
            # cheap empty check
            try:
                if micro_df.isEmpty():
                    return
            except AttributeError:
                if micro_df.limit(1).count() == 0:
                    return

            src = apply_dedup(micro_df, primary_keys=primary_keys, order_by_col=order_by_col)

            merge = generate_merge(
                primary_keys=primary_keys,
                target_table_name=table_name,
                input_df=src,
                smart_update=smart_update,  # streaming keeps smart updates enabled
                ignore_columns=ignore_columns,
                schema_evolution=enable_schema_evolution,
                created_columns=created_columns,
                updated_columns=updated_columns,
            )
            merge.execute()

        return _foreach_batch_standalone


# ────────────────────────────────────────────────────────────────────────
#                             Streaming variant
# ────────────────────────────────────────────────────────────────────────
@dataclass
class StreamingTableUpsert(_BaseTableUpsert, _BaseStreamingTableUpsert):
    """Continuously upserts incoming streaming micro-batches into the target Delta table.

    Using a MERGE operation, preserving existing rows and inserting new ones while
    updating changed columns as defined by the WriterUtilities helper.

    The class reuses the batch MERGE logic (`_run_merge`) inside a `foreachBatch`
    callback, ensuring that each micro-batch is processed idempotently.

    Attributes:
    ----------
    source_df : pyspark.sql.DataFrame
        The streaming source DataFrame to be merged into the target table.
    trigger_kwargs : Mapping[str, str]
        Spark Structured Streaming trigger options (e.g., `{"once": "true"}`).
    streaming_source : FileVolumeIdentifier | None
        Optional identifier of the streaming source volume, used for checkpointing.
    task_name : str
        Human-readable identifier for logging and monitoring purposes.
    """

    source_df: DataFrame
    trigger_kwargs: Mapping[str, str] = field(default_factory=dict)
    streaming_source: FileVolumeIdentifier | None = None
    task_name: str = field(init=False, default="StreamingTableUpsert")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Run the streaming upsert operation, merging each micro-batch into the target Delta table."""
        self._set_source_df(source_df)
        log = self.context.logger
        log.info(f"Streaming upsert → {self._full_table_name}")

        # Create a standalone foreachBatch function using the base class method
        foreach_batch_fn = self._create_standalone_foreach_batch(
            table_name=self._full_table_name,
            primary_keys=self._to_list(self.primary_keys),
            created_columns=self._to_list(self.created_columns),
            updated_columns=self._to_list(self.updated_columns),
            ignore_columns=self._to_list(self.ignore_columns),
            enable_schema_evolution=self.enable_schema_evolution,
            smart_update=self.smart_update,
            order_by_col=self.order_by_col,
        )

        # — start the stream (append mode; ΔLake handles idempotent MERGE)
        self._stream(
            self.source_df,
            output_mode="append",
            foreach_fn=foreach_batch_fn,
        )


@dataclass(kw_only=True)
class _BaseTableWriteSCDType2(_BaseTableWriter):
    """Mixin that adds SCD-Type-2 behaviour (history preserving).

    Args:
        context: Active :class:`~data_platform.etl.core.TaskContext`.
        table_model: Delta-table descriptor.
        source_df: Input DataFrame (static or streaming).
        primary_keys: Column(s) that uniquely identify a row.
        ignore_columns: Columns excluded from change detection.  When *None*,
            defaults to ``table_model.audit_columns``.  Accepts comma-separated
            string or an iterable of strings.
        writer_options: Spark write/stream options forwarded unchanged.
        drop_duplicates: If *True*, call ``dropDuplicates(primary_keys)`` on
            *source_df* before merging.
        valid_from_col: Name of the “row effective-from” timestamp column.
        valid_to_col: Name of the “row effective-to” timestamp column.
        current_col:   Name of the Boolean “is current” flag.

    Returns:
        None.  All effects are side-effects on the target Delta table.
    """

    source_df: DataFrame | None = None
    primary_keys: Sequence[str] | str = field(default_factory=list)
    ignore_columns: Sequence[str] | str | None = None

    writer_options: Mapping[str, str] = field(default_factory=dict)
    drop_duplicates: bool = False

    # lineage field names (override if schema differs)
    valid_from_col: str = "valid_from"
    valid_to_col: str = "valid_to"
    current_col: str = "is_current"

    # streaming knobs (only honoured by the Stream variant)
    trigger_kwargs: Mapping[str, str] = field(default_factory=dict)
    streaming_source: FileVolumeIdentifier | None = None

    def __post_init__(self) -> None:
        """Normalise ignore-column input and run parent initialiser."""
        if self.ignore_columns is None:
            self.ignore_columns = self.table_model.audit_columns or []
        self.ignore_columns = self._to_list(self.ignore_columns)
        # Add SCD Type 2 specific columns to ignore list
        scd2_lineage_cols = [self.valid_from_col, self.valid_to_col, self.current_col]
        self.ignore_columns = [*scd2_lineage_cols, *self.ignore_columns]
        super().__post_init__()

    def _run_merge_scd_type2(self, source_df: DataFrame) -> None:
        """Execute the complete SCD Type 2 merge process.

        Parameters:
        ----------
        source_df : DataFrame
            The source DataFrame to be merged.
        """
        # Apply deduplication if configured
        cleaned_source = self._apply_dedup(source_df)

        primary_keys = [c for c in self._to_list(self.primary_keys) if c not in self.ignore_columns]

        # Get target Delta table
        target = DeltaTable.forName(self.context.spark, self._full_table_name)

        # Get shared components
        update_columns = WriterUtilities.build_update_column_list(
            target,
            [
                *primary_keys,
                *self.ignore_columns,
            ],
        )
        insert_mapping = {col: f"s.{col}" for col in cleaned_source.columns if col not in self.ignore_columns}
        merge_condition = (
            " AND ".join(f"t.{col} <=> s.{col}" for col in primary_keys) + f" AND t.{self.valid_to_col} IS NULL"
        )

        self.context.logger.info(f"Starting SCD Type 2 merge for {self._full_table_name}")
        current_timestamp = f.current_timestamp()

        # Step 1: Handle soft deletes - close records not in source
        # First, close discontinued records separately
        target_df = self.context.spark.read.table(self._full_table_name)
        source_keys = cleaned_source.select(*primary_keys).distinct()

        # Find active records in target that don't exist in source
        active_target = target_df.filter(f.col(self.valid_to_col).isNull())

        # Anti-join to find records to soft delete
        to_close = active_target.join(source_keys, on=primary_keys, how="left_anti")

        if to_close.count() > 0:
            self.context.logger.info(f"Soft deleting {to_close.count()} discontinued records")

            # Update these records directly
            close_keys = to_close.select(*primary_keys).collect()

            for row in close_keys:
                close_condition = " AND ".join(f"t.{pk} <=> '{row[pk]}'" for pk in primary_keys)
                close_condition += f" AND t.{self.valid_to_col} IS NULL"

                target.alias("t").update(
                    condition=close_condition,
                    set={self.current_col: f.lit(False), self.valid_to_col: current_timestamp},
                )

        # Step 2: Standard SCD Type 2 merge for existing records
        change_condition = f.expr(" OR ".join(f"t.{col} IS DISTINCT FROM s.{col}" for col in update_columns))

        insert_values = {
            **insert_mapping,
            f"{self.valid_from_col}": current_timestamp,
            f"{self.valid_to_col}": f.lit(None),
            f"{self.current_col}": f.lit(True),
        }

        # Close changed records and insert new ones
        (
            target.alias("t")
            .merge(cleaned_source.alias("s"), merge_condition)
            .whenMatchedUpdate(
                condition=change_condition,
                set={f"{self.current_col}": f.lit(False), f"{self.valid_to_col}": current_timestamp},
            )
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

        # Step 3: Insert new versions of changed records
        # Re-merge to insert new versions for updated records
        (
            target.alias("t")
            .merge(cleaned_source.alias("s"), merge_condition)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

        self.context.logger.info(f"SCD Type 2 merge completed for {self._full_table_name}")


# ────────────────────────────────────────────────────────────────────────
#                              Batch Variant
# ────────────────────────────────────────────────────────────────────────
@dataclass
class BatchTableWriteSCDType2(_BaseTableWriteSCDType2):
    """One-off SCD-Type-2 merge on a static DataFrame."""

    task_name: str = field(init=False, default="BatchTableWriteSCDType2")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Run the batch Type-2 upsert.

        Args:
            source_df: Optional DataFrame overriding the constructor value.

        Returns:
            None.
        """
        self._set_source_df(source_df)
        self._run_merge_scd_type2(self.source_df)


# ────────────────────────────────────────────────────────────────────────
#                             Streaming variant
# ────────────────────────────────────────────────────────────────────────
@dataclass
class StreamTableWriteSCDType2(_BaseTableWriteSCDType2):
    """Continuous Type-2 merge executed per micro-batch."""

    task_name: str = field(init=False, default="StreamTableWriteSCDType2")

    def execute(self, source_df: DataFrame | None = None) -> None:
        """Start the Structured-Streaming Type-2 writer.

        Args:
            source_df: Optional streaming DataFrame overriding the
                constructor value.

        Returns:
            None.
        """
        self._set_source_df(source_df)

        def _foreach_batch(micro_df: DataFrame, _: int) -> None:
            """Per-batch callback that applies the Type-2 merge.

            Args:
                micro_df: The micro-batch DataFrame.
                _: Spark supplies the batch-id (ignored).

            Returns:
                None.
            """
            self._run_merge_scd_type2(micro_df)

        self._stream(
            self.source_df,
            output_mode="append",
            foreach_fn=_foreach_batch,
        )


def generate_dedup_merge_sql(primary_keys: list, target_table_name: str, source_table: str, input_df: DataFrame) -> str:
    """Generate a MERGE SQL statement for deduplication and upserting data into a target table.

    This function creates a SQL MERGE statement that inserts rows from a source table into a
    target table only when the primary key combination does not already exist in the target table.

    Args:
        primary_keys (list): A list of column names that form the primary key for matching records.
        target_table_name (str): The name of the target table where data will be merged.
        source_table (str): The name of the source table or temporary table containing the data to merge.
        input_df (DataFrame): The Spark DataFrame used to extract column names for the INSERT clause.

    Returns:
        str: A SQL MERGE statement string that can be executed against the database.

    Example:
        >>> primary_keys = ["id", "date"]
        >>> sql = generate_dedup_merge_sql(
        ...     primary_keys=primary_keys, target_table_name="customers", source_table="staging_customers", input_df=df
        ... )
    """
    # Get columns from Spark DataFrame
    columns = input_df.columns

    # Create join condition for primary keys
    join_condition = " AND ".join([f"{target_table_name}.{pk} = {source_table}.{pk}" for pk in primary_keys])

    # Create insert columns string
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"{source_table}.{col}" for col in columns])

    # Compose SQL string
    sql_query = f"""
                MERGE INTO {target_table_name}
                USING {source_table}
                ON {join_condition}
                WHEN NOT MATCHED
                THEN INSERT ({insert_columns})
                VALUES ({insert_values})
                """.strip()

    return sql_query


def generate_merge(
    target_table_name: str,
    input_df: DataFrame,
    primary_keys: list,
    smart_update: bool = True,
    updated_at_guard: str | None = None,  # e.g. "s.`updated_at` >= t.`updated_at`"
    schema_evolution: bool = False,
    ignore_columns: Sequence[str] | None = (
        "table_id",
        "_source_file",  # avoid putting audit cols here if you want to insert them
    ),
    created_columns: Sequence[str] | None = ("created_at", "created_by"),
    updated_columns: Sequence[str] | None = ("updated_at", "updated_by"),
    updated_defaults: Mapping[str, object] | None = None,  # values are Columns, e.g., f.current_timestamp()
) -> object:
    """Build a Delta MERGE using the PySpark DeltaTable builder API (no SQL) to perform dedup-aware upserts.

    This function constructs a MERGE that:
    - Matches rows using null-safe equality on primary keys (t.pk <=> s.pk). If you do not
      want NULL primary keys to match, filter them out in the source DataFrame.
    - Detects changes with a null-safe predicate across non-PK, non-ignored, and non-audit
      columns (unless only_update_when_changed is False).
    - Excludes ignore_columns from INSERT, UPDATE, and change detection.
    - Treats created_columns as insert-only (excluded from UPDATE and change detection) but
      available for INSERT when present in the source DataFrame.
    - Ensures updated_columns are always included in the UPDATE set when changes are applied.
      If a column in updated_columns is missing from the source DataFrame, a default value
      from updated_defaults is used (e.g., current_timestamp for updated_at and current_user
      for updated_by).
    - Never updates primary key columns.

    Args:
        primary_keys (list[str]): Column names that uniquely identify a row for matching.
            The join uses null-safe equality (t.pk <=> s.pk). If you do not want NULL
            primary keys to match, filter them out upstream.
        target_table_name (str): Fully qualified Delta table name to merge into (for example,
            "catalog.schema.table").
        input_df (DataFrame): Source DataFrame to merge from. Can be a batch DataFrame or
            the micro-batch passed to foreachBatch in Structured Streaming.
        smart_update (bool): If True, update matched rows only when any
            non-PK, non-ignored, non-audit column differs (using a null-safe predicate).
            If False, update all matched rows. Defaults to True.
        updated_at_guard (str | None): Optional additional condition that must be true to
            update matched rows (for example, "s.updated_at >= t.updated_at"). Defaults to None.
        ignore_columns (Sequence[str] | None): Columns to hard-ignore. Ignored columns are
            not considered in change detection, are not copied in UPDATE, and are not inserted.
            Do not include created_at or updated_at here if you want them to be inserted or
            updated. Defaults to ("table_id", "_source_file").
        schema_evolution (bool): If True, call .withSchemaEvolution() on the merge builder to
            enable automatic schema updates for this MERGE. Defaults to False.
        created_columns (Sequence[str] | None): Audit fields that are treated as insert-only.
            They are excluded from change detection and UPDATE, but can be inserted if present
            in the source DataFrame. Defaults to ("created_at", "created_by").
        updated_columns (Sequence[str] | None): Audit fields that are updated when changes are
            applied. They are excluded from change detection but always included in the UPDATE
            set. Defaults to ("updated_at", "updated_by").
        updated_defaults (Mapping[str, Column] | None): Default expressions for updated_columns
            that are missing in the source DataFrame. If not provided, the following defaults
            apply:
            - "updated_at": pyspark.sql.functions.current_timestamp()
            - "updated_by": pyspark.sql.functions.expr("current_user()")
            You may override or extend this mapping (for example, {"last_modified_at": f.current_timestamp()}).
            Defaults to None.

    Returns:
        object: A Delta merge builder. Call .execute() on the returned builder to run the MERGE.

    Notes:
        - Change detection excludes the union of: primary_keys, ignore_columns, created_columns,
          and updated_columns.
        - The UPDATE set excludes: primary_keys, ignore_columns, and created_columns. Then
          updated_columns are explicitly added:
            * If present in the source DataFrame: their values are copied from the source.
            * If missing: a default from updated_defaults is used (or the built-in defaults).
          Primary keys are never updated.
        - INSERT values include any column present in the source DataFrame except those in
          ignore_columns. To insert created_columns overriding the table definition columns (not recommended),
            keep them out of ignore_columns and ensure they exist in the source if not they will be derived.

    Example:
        builder = generate_dedup_merge(
            primary_keys=["id"],
            target_table_name="main.core.customers",
            input_df=df,
            smart_update=True,
            updated_at_guard="s.updated_at >= t.updated_at",
            ignore_columns=("table_id", "_source_file"),
            schema_evolution=True,
            created_columns=("created_at", "created_by"),
            updated_columns=("updated_at", "updated_by"),
            # Optional override for defaults:
            # updated_defaults={"updated_at": f.current_timestamp(), "updated_by": f.expr("current_user()")},
        )
        builder.execute()
    """
    # Normalize inputs
    primary_keys = list(primary_keys or [])
    ignore_columns = list(ignore_columns or [])
    created_columns = list(created_columns or [])
    updated_columns = list(updated_columns or [])
    updated_defaults = dict(updated_defaults or {})

    # Built-in defaults for audit fields if not provided
    updated_defaults.setdefault("updated_at", f.current_timestamp())
    updated_defaults.setdefault("updated_by", f.expr("current_user()"))

    spark = input_df.sparkSession
    target = DeltaTable.forName(spark, target_table_name)

    target_cols = target.toDF().columns
    target_cols_set = set(target_cols)
    source_cols_set = set(input_df.columns)

    # Helper for quoting column names in expressions
    def q(c: str) -> str:
        return f"`{c}`"

    # Columns excluded from change detection
    excluded_for_diff = set(primary_keys) | set(ignore_columns) | set(created_columns) | set(updated_columns)
    diff_cols = [c for c in target_cols if c not in excluded_for_diff]

    # INSERT: exclude ignore columns; include new columns when schema evolution is enabled
    insert_src_cols = [
        c for c in input_df.columns if c not in set(ignore_columns) and (schema_evolution or c in target_cols_set)
    ]
    insert_vals = {c: f.expr(f"s.{q(c)}") for c in insert_src_cols}

    # UPDATE base: exclude PK + ignore + created (do not update created_* on matched rows)
    excluded_for_update = set(primary_keys) | set(ignore_columns) | set(created_columns)

    # New columns present only in source (eligible for schema evolution on UPDATE)
    new_cols_for_update = [
        c
        for c in input_df.columns
        if schema_evolution
        and c not in target_cols_set
        and c not in excluded_for_update
        and c not in set(updated_columns)
    ]

    # Existing target columns to update
    base_update_cols = [c for c in input_df.columns if c not in excluded_for_update and c in target_cols_set]

    # If schema evolution is enabled, also include new columns from the source in UPDATE
    if schema_evolution and new_cols_for_update:
        base_update_cols += new_cols_for_update

    update_set = {c: f.expr(f"s.{q(c)}") for c in base_update_cols}

    # Ensure updated_columns won't include primary keys
    updated_cols_non_pk = [c for c in updated_columns if c not in set(primary_keys)]

    # Add audit columns to UPDATE set only if they already exist on the target
    for c in updated_cols_non_pk:
        if c in target_cols_set:
            if c in source_cols_set:
                update_set[c] = f.expr(f"s.{q(c)}")
            else:
                default_expr = updated_defaults.get(c)
                if default_expr is not None:
                    update_set[c] = default_expr
        # If audit column not on target, skip to avoid unresolved expressions

    # Change detection predicate
    if smart_update and diff_cols:
        # Null-safe difference across diff columns
        diffs = [f"NOT (t.{q(c)} <=> s.{q(c)})" for c in diff_cols]
        change_pred = f.expr(" OR ".join(diffs))
    else:
        change_pred = f.lit(True)  # update all matched rows

    # If evolving schema with new columns, update matched rows when any new column is non-null in source
    if schema_evolution and new_cols_for_update:
        evo_pred = f.expr(" OR ".join([f"s.{q(c)} IS NOT NULL" for c in new_cols_for_update]))
        change_pred = change_pred | evo_pred

    # Optional guard (e.g., "s.updated_at >= t.updated_at")
    if updated_at_guard:
        change_pred = change_pred & f.expr(updated_at_guard)

    # Null-safe PK join
    merge_cond = " AND ".join([f"t.{q(pk)} <=> s.{q(pk)}" for pk in primary_keys])

    # Build merge
    merge_builder = target.alias("t").merge(input_df.alias("s"), merge_cond)

    if update_set:
        merge_builder = merge_builder.whenMatchedUpdate(condition=change_pred, set=update_set)

    merge_builder = merge_builder.whenNotMatchedInsert(values=insert_vals)

    if schema_evolution:
        merge_builder = merge_builder.withSchemaEvolution()

    return merge_builder


def apply_dedup(df: DataFrame, primary_keys: list, order_by_col: str) -> DataFrame:
    """Deduplicate DataFrame based on primary keys.

    If primary_keys are provided, performs deduplication. When order_by_col is present
    in the DataFrame, performs latest-wins deduplication per primary key using a window
    with row_number(). Otherwise, falls back to dropDuplicates(primary_keys).

    Args:
        df (DataFrame): Input DataFrame to potentially deduplicate.
        primary_keys (list): List of column names defining the primary key.
        order_by_col (str): Column name to order by for latest-wins deduplication.

    Returns:
        DataFrame: Deduplicated DataFrame if primary_keys are provided; otherwise original df.
    """
    if not primary_keys:
        return df
    # Prefer latest-wins if order_by_col exists in the DataFrame; else do simple dedup
    if order_by_col and order_by_col in df.columns:
        w = Window.partitionBy([f.col(c) for c in primary_keys]).orderBy(f.col(order_by_col).desc_nulls_last())
        deduped = df.withColumn("_rn", f.row_number().over(w)).filter(f.col("_rn") == 1).drop("_rn")
        return deduped
    return df.dropDuplicates(primary_keys)
