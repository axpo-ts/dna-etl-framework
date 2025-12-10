from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from data_model.abstract_table_model import AbstractTableModel
from data_model.file_volume_identifier import FileVolumeIdentifier
from data_model.table_identifier import TableIdentifier
from etl.core import TaskContext
from etl.load.create_table import CreateTable
from etl.load.update_table_metadata import UpdateTableMetadata


@dataclass(frozen=True, slots=True)
class StaticTableModel(AbstractTableModel):
    """A *value object* that fulfils the `AbstractTableModel`.

    One-liner definitions instead of one class per table:

        bronze_users = StaticTableModel(
            identifier     = TableIdentifier("lake", "bronze", "users"),
            schema         = user_schema,
            comment        = "Raw user events coming from the API",
            partition_cols = ("ingest_date",),
            primary_keys   = ("user_id",),
        )

    The dataclass is declared frozen and with slots; that protects the
    definition from accidental mutations and makes each instance lightweight.    Configuration-driven workflows:
    Because a StaticTableModel is nothing but data, you can read a YAML or
    JSON file and do:

        table = StaticTableModel(**cfg_from_file)

    Without requiring you to create a dedicated subclass for every table.
    Creating a table definition is now as simple as instantiating this
    class with the identifier, schema and (optionally) any extra metadata.



    """

    identifier: TableIdentifier
    schema: StructType = None
    comment: str | None = None
    sources: Sequence[TableIdentifier, FileVolumeIdentifier] = None
    partition_cols: Sequence[str] = field(default_factory=tuple)
    primary_keys: Sequence[str] = field(default_factory=tuple)
    liquid_cluster_cols: Sequence[Sequence[str]] = field(default_factory=tuple)
    tags: dict[str, str] | None = None
    license: str | None = None
    audit_columns: tuple[str, ...] | None = None
    constraints: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Post-initialization to set default audit_columns if not provided."""
        if self.audit_columns is None:
            # Determine default based on catalog
            catalog = self.identifier.catalog.lower()
            if catalog in {"silver", "gold"}:
                default_audit = (*self._BASE_AUDIT_COLS, "table_id")
            else:
                default_audit = self._BASE_AUDIT_COLS

            # Since dataclass is frozen, we need to use object.__setattr__
            object.__setattr__(self, "audit_columns", default_audit)

    def create(self, context: TaskContext) -> None:
        """Create the table in Unity Catalog."""
        CreateTable(context=context, table_model=self).execute()

    def drop(self, context: TaskContext) -> None:
        """Drop the table if it exists."""
        name = context.full_unitycatalog_name(self.identifier)
        try:
            sql = f"DROP TABLE IF EXISTS {name};"
            context.logger.info(f"{sql}")
            context.spark.sql(sql)
        except Exception as e:
            context.logger.error(f"Failed to drop table {name}: {e}")
            raise

    def update_metadata(self, context: TaskContext) -> None:
        """Update the table metadata in Unity Catalog."""
        UpdateTableMetadata(context=context, table_model=self).execute()

    def write(
        self,
        context: TaskContext,
        df: DataFrame,
        writer_class: type,
        **kwargs: Any,
    ) -> None:
        """Dynamically write DataFrame using specified writer class.

        This method provides a flexible interface to all table writers using
        either class names or class types, with dynamic parameter passing.

        Args:
            context: Task execution context with Spark session and logger.
            df: Source DataFrame to write to the table.
            writer_class: Writer class type. Examples:
                - BatchTableAppend
                - BatchTableUpsert
                - BatchTableWriteSCDType2
                - StreamingTableAppend
            **kwargs: Additional keyword arguments passed to the writer class.
                Common parameters include:
                - primary_keys: Sequence of primary key columns
                - drop_duplicates: Whether to deduplicate before writing
                - writer_options: Dict of writer-specific options
                - For SCD Type 2: valid_from_col, valid_to_col, current_col
                - For streaming: trigger_kwargs, streaming_source

        Raises:
            ImportError: If the writer class cannot be imported.
            TypeError: If invalid arguments are passed to the writer.
            RuntimeError: If the write operation fails.

        Examples:
            Simple append with class name:
            >>> table_model.write(context, df, BatchTableAppend)

            Upsert with custom parameters:
            >>> table_model.write(context, df, BatchTableUpsert, primary_keys=["id", "timestamp"], drop_duplicates=True)

            SCD Type 2 with class type:
            >>> from etl.load.table_writer import BatchTableWriteSCDType2
            >>> table_model.write(
            ...     context, df, BatchTableWriteSCDType2, valid_from_col="effective_date", current_col="is_active"
            ... )

            Streaming with dynamic parameters:
            >>> table_model.write(
            ...     context,
            ...     df,
            ...     StreamingTableAppend,
            ...     trigger_kwargs={"processingTime": "30 seconds"},
            ...     writer_options={"checkpointLocation": "/tmp/checkpoint"},
            ... )
        """
        from etl.load import table_writer

        try:
            # Prepare common parameters that most writers expect
            writer_params = {
                "context": context,
                "table_model": self,
            }

            # Add source_df parameter (different writers use different names)
            if "source_df" in writer_class.__init__.__annotations__:
                writer_params["source_df"] = df
            elif "df" in writer_class.__init__.__annotations__:
                writer_params["df"] = df

            # Use table's primary_keys if not specified
            if "primary_keys" not in kwargs and self.primary_keys:
                kwargs["primary_keys"] = self.primary_keys

            # Merge all parameters
            writer_params.update(kwargs)

            # Create writer instance and execute
            writer_instance = writer_class(**writer_params)
            writer_instance.execute()

            context.logger.info(f"Successfully wrote data to {self.identifier} using {writer_class.__name__}")

        except ImportError as e:
            context.logger.error(f"Failed to import writer: {e}")
            raise

        except TypeError as e:
            context.logger.error(f"Invalid parameters for writer {writer_class!s}: {e}")
            # Provide helpful error message with expected parameters
            if hasattr(table_writer, writer_class):
                writer_class = getattr(table_writer, writer_class)
                expected_params = list(writer_class.__init__.__annotations__.keys())
                context.logger.error(f"Expected parameters: {expected_params}")
            raise

        except Exception as e:
            context.logger.error(f"Failed to write to table {self.identifier}: {e}")
            raise RuntimeError(f"Write operation failed for table {self.identifier}") from e
