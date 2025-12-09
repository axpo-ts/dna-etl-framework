import atexit
from dataclasses import dataclass, field
from logging import Logger
from typing import Any

from opentelemetry import _logs as logs
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from pyspark.sql import SparkSession

from data_platform.common import get_dbutils
from data_platform.data_model import AbstractTableModel, FileVolumeIdentifier
from data_platform.data_model.unity_catalog_identifier import UnityCatalogIdentifier
from data_platform.etl.core.config_loader import ConfigLoader
from data_platform.etl.core.config_logging import init_logging
from data_platform.etl.core.config_logging_azure import close_azure_logger
from data_platform.etl.core.utils import get_databricks_environment


@dataclass
class TaskContext:
    """A container for managing task-related context.

    This class provides a way to store and retrieve properties within different namespaces
    to manage task-related configurations and state.

    Args:
        configuration (Configuration): The task configuration.
        logger (Logger): The logger instance.
        spark (Union[SparkSession, None]): The Spark session, if available, otherwise None.

    Attributes:
        __state (Dict[str, Dict[str, Any]]): A dictionary to store properties organized by namespaces.
    """

    logger: Logger = None
    logger_level: str = "INFO"  # Default logging level INFO | WARNING | DEBUG
    spark: SparkSession = None
    config_loader: ConfigLoader = None
    dbutils: Any = None
    __state: dict[str, dict[str, Any]] = field(default_factory=dict)
    catalog_prefix: str = None
    schema_prefix: str = None

    def __post_init__(self) -> None:
        """Initialize the task context with default values."""
        if self.dbutils is None:
            self.dbutils = get_dbutils(self.spark)
            if not self.dbutils:
                raise ValueError("Could not load Databricks utils!")

        if self.spark is None:
            self.spark = SparkSession.builder.getOrCreate()

        if self.catalog_prefix is None:
            self.catalog_prefix = ""

        if self.schema_prefix is None:
            self.schema_prefix = ""

        env = get_databricks_environment()

        if self.logger is None:
            self.logger = init_logging(dbutils=self.dbutils, spark=self.spark, logger_level=self.logger_level)

        if env == "PROD":
            # Register flush hook if Batch processor is in use
            provider = logs.get_logger_provider()
            if isinstance(provider, LoggerProvider):
                for proc in getattr(provider, "_active_log_record_processors", []):
                    if isinstance(proc, BatchLogRecordProcessor):
                        atexit.register(close_azure_logger)
                        break

    # one helper that every task can reuse
    def full_unitycatalog_name(self, ident: UnityCatalogIdentifier) -> StopAsyncIteration:
        """Return a new identifier with catalog / schema prefixes applied.

        * If `self.catalog_prefix` contains “dev” (case-insensitive) we also
          prepend `self.schema_prefix` to the schema.
        * In both cases we always apply the catalog prefix.
        """
        dev_mode = "dev" in self.catalog_prefix.lower()

        catalog = _prefixed(self.catalog_prefix, ident.catalog) if ident.dna_owned else ident.catalog
        schema = _prefixed(self.schema_prefix, ident.schema) if dev_mode and ident.dna_owned else ident.schema

        return str(UnityCatalogIdentifier(catalog=catalog, schema=schema, name=ident.name))

    def full_file_name(self, source: dict[str, str]) -> str:
        """Build a POSIX path like “/Volumes/{catalog}/{schema}/{volume}”."""
        dev_mode = "dev" in self.catalog_prefix.lower()

        catalog = _prefixed(self.catalog_prefix, source.catalog)
        schema = _prefixed(self.schema_prefix, source.schema) if dev_mode else source.schema
        return str(FileVolumeIdentifier(catalog=catalog, schema=schema, name=source.name, file_path=source.file_path))

    def _full_source_filename_of(self, model: AbstractTableModel, index: int) -> str:
        """Return the full file name for a given table model."""
        return str(self.contex.full_source_filename(model.sources[index]))

    def put_property(self, namespace: str, key: str, value: Any) -> None:
        """Adds or updates a property in the specified namespace.

        Args:
            namespace (str): The namespace of the property.
            key (str): The key of the property.
            value (Any): The value of the property.
        """
        if self.__state.get(namespace):
            self.__state[namespace].update({key: value})
        else:
            self.__state[namespace] = {key: value}

    def get_property(self, namespace: str, key: str, optional: bool = False) -> Any:
        """Retrieves a property value from the specified namespace and key.

        Args:
            namespace (str): The namespace of the property.
            key (str): The key of the property.
            optional (bool, optional): Whether the property is optional. Defaults to False.

        Returns:
            Any: The value of the property.

        Raises:
            KeyError: If the property for the given key and namespace is not found.
        """
        try:
            return self.__state.get(namespace, {}).get(key) if optional else self.__state[namespace][key]
        except KeyError:
            raise KeyError(f"Property for key: '{key}' in namespace: '{namespace}' not found")

    def print_state(self) -> None:
        """Logs the contents of __state in a readable format."""
        if not self.__state:
            self.logger.info("No properties set in TaskContext.")
            return
        self.logger.info("TaskContext State:")
        for namespace, props in self.__state.items():
            self.logger.info(f"  Namespace: {namespace}")
            for key, value in props.items():
                self.logger.info(f"    {key}: {value!r}")


def _prefixed(prefix: str, value: str | None) -> str | None:
    """Return `prefix + value` when `value` is truthy, else `None`."""
    return f"{prefix}{value}" if value else None
