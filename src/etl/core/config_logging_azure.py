import json
import logging
import os
from typing import Any

from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter  # type: ignore
from opentelemetry import _logs as azure_logs  # type: ignore
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler  # type: ignore
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor  # type: ignore
from pyspark.sql import SparkSession


class ContextFilter(logging.Filter):
    """A logging filter that enriches LogRecord objects with Databricks context.

    This filter attaches Databricks-specific context information (workspace id,
    cluster id, job id, run id, and task name) to each LogRecord. These values
    are added as top-level attributes so that structured logging/telemetry
    exporters (e.g. Azure Monitor) can ingest them.
    """

    def __init__(self, spark: SparkSession, dbutils: Any) -> None:
        """Initialize the ContextFilter with SparkSession and dbutils.

        Args:
            spark: SparkSession instance to read Spark configs.
            dbutils: Databricks dbutils object (from get_dbutils).

        Raises:
            Exception: If Databricks context cannot be retrieved or parsed.
        """
        super().__init__()
        self.spark = spark
        self.dbutils = dbutils

        # Extract Databricks task context as JSON
        task_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson()
        task_ctx_dict = json.loads(task_ctx)

        # Lazy import to avoid issues outside Databricks
        from dbruntime.databricks_repl_context import get_context

        repl_context = get_context()

        # Workspace and cluster identifiers
        self.workspace_id = getattr(repl_context, "workspaceId", None)
        self.cluster_id = getattr(repl_context, "clusterId", None)

        # Job and run identifiers
        self.job_id = getattr(repl_context, "jobId", None)
        self.job_name = task_ctx_dict.get("tags", {}).get("jobName", "no_job_id")
        self.job_run_id = task_ctx_dict.get("tags", {}).get("jobRunId", "no_job_id")

        # Task identifiers
        self.task_run_id = getattr(repl_context, "currentRunId", None)
        self.task_name = task_ctx_dict.get("tags", {}).get("taskKey", "no_run_id")
        self.work_load_id = getattr(repl_context, "workloadId", None)

    def filter(self, record: logging.LogRecord) -> bool:
        """Enrich a LogRecord with Databricks context attributes.

        Args:
            record: The LogRecord instance to enrich.

        Returns:
            bool: Always True, indicating the record should be logged.
        """
        record.workspace_id = self.workspace_id
        record.cluster_id = self.cluster_id
        record.job_id = self.job_id
        record.job_name = self.job_name
        record.job_run_id = self.job_run_id
        record.task_run_id = self.task_run_id
        record.task_name = self.task_name
        record.work_load_id = self.work_load_id
        return True


def _setup_azure_exporter(
    base_logger: logging.Logger, connection_string: str, exporter_cls: type | None = None
) -> None:
    """Configure the Azure Monitor OpenTelemetry exporter.

    This sets up the OpenTelemetry LoggerProvider, attaches the Azure Monitor
    exporter, and adds a LoggingHandler to the root logger.

    Args:
        base_logger: The base application logger to log configuration messages.
        connection_string: Azure Application Insights connection string.
        exporter_cls: Optional injection point for a custom exporter class
            (useful for testing).
    """
    provider = LoggerProvider()
    azure_logs.set_logger_provider(provider)

    exporter_cls = exporter_cls or AzureMonitorLogExporter
    exporter = exporter_cls(connection_string=connection_string)
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

    otel_handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)
    logging.getLogger().addHandler(otel_handler)

    base_logger.info("Configured PROD logging → Console + Azure Monitor")


def setup_azure_logger(base_logger: logging.Logger, conn_str: str | None, spark: SparkSession, dbutils: Any) -> None:
    """Configure Azure Monitor logging and attach Databricks context filter.

    In production environments, this function enables Azure Monitor logging if a
    connection string is available (either passed explicitly or via the
    APPLICATIONINSIGHTS_CONNECTION_STRING environment variable). It also
    attaches the ContextFilter to enrich log records with Databricks metadata.

    Behavior:
        - If no connection string is found, logs an error and continues with
          console logging only.
        - If exporter setup fails, logs the exception and continues with console
          logging.
        - If attaching the ContextFilter fails, logs a warning but does not raise.

    Args:
        base_logger: The base application logger to configure.
        conn_str: Optional Azure Application Insights connection string.
        spark: Active SparkSession instance.
        dbutils: Databricks dbutils object.
    """
    connection_string = conn_str or os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not connection_string:
        base_logger.error("APPLICATIONINSIGHTS_CONNECTION_STRING not set in PROD; Azure exporter not configured")
        return

    try:
        _setup_azure_exporter(base_logger, connection_string)
    except Exception:
        base_logger.exception("Failed to configure Azure Monitor exporter; continuing with console logging")

    # Attach ContextFilter to logger using initialized spark and dbutils
    try:
        context_filter = ContextFilter(spark, dbutils)
        base_logger.addFilter(context_filter)
    except Exception as exc:
        # Avoid failing context init for logging issues
        base_logger.warning("Failed to attach ContextFilter to logger: %s", exc)


def close_azure_logger() -> None:
    """Flush buffered logs at interpreter exit if Batch processor is used."""
    provider = azure_logs.get_logger_provider()
    if isinstance(provider, LoggerProvider):
        provider.shutdown()
