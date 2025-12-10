import logging
import os
import sys
import warnings
from typing import Any

from pyspark.sql import SparkSession

from etl.core.config_logging_azure import setup_azure_logger
from etl.core.utils import get_databricks_environment, loganalytics_conn_str


# The notebook `src2brz_montel_nordics_trades_merge` has been failing due to excessive log output.
# Recently, libraries started emitting many DeprecationWarnings, indicating the need to update deps
# and replace deprecated API calls. For now, we suppress these warnings to keep the job stable,
# but this is a temporary workaround. Someone should:
#   1) Update dependencies (PySpark/pandas/pyarrow/etc.) to versions that remove these deprecations.
#   2) Fix code paths that rely on deprecated functions.
#   3) Review logging configuration and application logs to reduce verbosity and print only essentials.
def _configure_warning_filters() -> None:
    """Silence known noisy DeprecationWarnings from PySpark / distutils."""
    warnings.filterwarnings("ignore", category=DeprecationWarning, message=r".*is_categorical_dtype is deprecated.*")
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message=r".*'importlib.abc.Traversable' is deprecated and slated for removal.*",
    )
    warnings.filterwarnings(
        "ignore", category=DeprecationWarning, message=r".*distutils Version classes are deprecated.*"
    )
    warnings.filterwarnings("ignore", category=DeprecationWarning, message=r".*is_datetime64tz_dtype is deprecated.*")
    warnings.filterwarnings(
        "ignore", category=ResourceWarning, message=r".*Enable tracemalloc to get the object allocation traceback.*"
    )
    warnings.filterwarnings(
        "ignore", category=ResourceWarning, message=r".*unclosed transport <_SelectorSocketTransport.*"
    )
    warnings.filterwarnings("ignore", category=ResourceWarning, message=r".*unclosed <socket.socket.*")

    # Ensure child processes respect the same policy
    os.environ.setdefault("PYTHONWARNINGS", "ignore::DeprecationWarning")


def _configure_stdlib_logging(logger_level: str) -> None:
    """Configure the Python standard library logging system.

    This sets up the global logging configuration with a consistent format,
    timestamp, and output stream. It uses `force=True` to ensure any existing
    logging configuration is overridden, which is useful in environments like
    Databricks notebooks where logging may already be initialized.

    Args:
        logger_level: The minimum log level to capture (e.g. "DEBUG", "INFO",
            "WARNING", "ERROR").
    """
    _configure_warning_filters()

    logging.basicConfig(
        level=logger_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,  # Overwrites any existing logging configuration
    )


#: Default log levels for selected libraries to reduce noise in logs.
LIBRARY_LOG_LEVELS = {
    "py4j": "ERROR",
    "delta": "WARNING",
    "httpx": "WARNING",
    "azure": "WARNING",
}


def _configure_library_log_levels() -> None:
    """Apply custom log levels to selected libraries.

    This function iterates over the `LIBRARY_LOG_LEVELS` mapping and sets the
    specified log level for each library logger. The goal is to suppress overly
    verbose output from common dependencies while still allowing important
    warnings and errors to surface.

    Example:
        - "py4j" logs are restricted to ERROR only.
        - "delta", "httpx", and "azure" logs are restricted to WARNING or higher.
    """
    for lib, level in LIBRARY_LOG_LEVELS.items():
        logging.getLogger(lib).setLevel(level)


def setup_base_logger(logger_level: str = "INFO") -> logging.Logger:
    """Configure and return the base ETL logger.

    This function sets up the standard library logging configuration for the
    application, applies noise reduction for common third party libraries, and
    returns a named logger (`dna.etl`). It is idempotent: if the logger has
    already been configured with handlers, it will not reconfigure logging
    again, but will update the logger's level.

    Args:
        logger_level: The minimum log level to capture (e.g. "DEBUG", "INFO",
            "WARNING", "ERROR"). Defaults to "INFO".

    Returns:
        logging.Logger: The configured base logger instance for the ETL
        application.
    """
    base_logger = logging.getLogger("dna.etl")
    # If handlers are already present, just update the level and return
    if base_logger.handlers:  # already configured
        base_logger.setLevel(logger_level)
        return base_logger

    _configure_stdlib_logging(logger_level)
    _configure_library_log_levels()

    base_logger.setLevel(logger_level)
    return base_logger


def init_logging(
    dbutils: Any, spark: SparkSession, logger_level: str = "INFO", use_azure: bool = True
) -> logging.Logger:
    """Initialize and configure the logging context for Databricks notebooks.

    This function sets up the base ETL logger with standard formatting and log
    levels, determines the current Databricks environment (e.g. PROD vs NON_PROD),
    and conditionally enables Azure Monitor integration. In production, if
    `use_azure` is True and a valid Log Analytics connection string is available,
    the logger is extended with Azure Monitor exporters and enriched with
    Databricks context (cluster, job, task metadata). In non-production or when
    Azure logging is disabled, only console logging is configured.

    Args:
        dbutils: The Databricks `dbutils` object, used to retrieve secrets and
            contextual metadata.
        spark: Active SparkSession, required for attaching Databricks context
            filters when Azure logging is enabled.
        logger_level: Minimum log level for the base logger (e.g. "DEBUG",
            "INFO", "WARNING", "ERROR"). Defaults to "INFO".
        use_azure: Whether to enable Azure Monitor logging in PROD. Defaults to True.

    Returns:
        logging.Logger: The configured base logger instance for the ETL application.
    """
    env = get_databricks_environment()
    conn_str = loganalytics_conn_str(dbutils, env)

    log = setup_base_logger(logger_level=logger_level)
    # Attach Azure Monitor handler if in production and enabled
    if env == "PROD" and use_azure:
        setup_azure_logger(log, conn_str, spark, dbutils)

    else:
        log.info(f"Configured {env} logging → Console only (Azure disabled)")

    return log
