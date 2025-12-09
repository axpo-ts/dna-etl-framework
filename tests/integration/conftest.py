import logging
import random
import string

# from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any  # , Callable

import pytest
from databricks.connect import DatabricksSession  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from data_platform.common import get_dbutils


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a DatabricksSession for tests using Databricks Connect."""
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def root_dir() -> str:
    """Returns the root directory path for use in finding directories on Databricks."""
    p = str(Path(__file__).parents[0])
    return p.split("/tests/integration")[0]


@pytest.fixture(scope="session")
def logger() -> logging.Logger:
    """Returns a logger for debugging inside tests."""
    return logging.getLogger(__name__)


@pytest.fixture(scope="session")
def id_generator() -> str:
    """Generate a reusable random prefix for schema names in a session-scoped fixture."""
    secure_random = random.SystemRandom()
    size = 6
    chars = string.ascii_uppercase + string.digits
    return "".join(secure_random.choice(chars) for _ in range(size))


@pytest.fixture(scope="session")
def test_dataframe(spark: SparkSession) -> DataFrame:
    """Creates a sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("last_login", TimestampType(), True),
        ]
    )
    data = [
        (1, "Alice", 30, datetime.fromisoformat("2024-11-06T12:34:56.789")),
        (2, "Bob", 25, datetime.fromisoformat("2024-11-05T11:33:55.678")),
        (3, "Charlie", 35, datetime.fromisoformat("2024-11-04T10:32:54.567")),
        (4, "Diana", 28, datetime.fromisoformat("2024-11-03T09:31:53.456")),
        (5, "Eve", 22, datetime.fromisoformat("2024-11-02T08:30:52.345")),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def create_schema(spark: SparkSession, logger: logging.Logger) -> Any:
    """Fixture to create and drop a schema, with catalog support for Unity Catalog."""

    def _create_schema(catalog_name: str, schema_name: str) -> str:
        full_schema_name = f"{catalog_name}.{schema_name}"
        logger.info("Creating schema: %s", full_schema_name)

        # Check if catalog exists
        catalog_exists = spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'").count() > 0
        if not catalog_exists:
            raise ValueError(f"Catalog '{catalog_name}' not found. Please check catalog configuration.")

        # Create the schema in the specified catalog
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}")
        return full_schema_name

    # Cleanup logic within the fixture
    def cleanup(catalog_name: str, schema_name: str) -> None:
        full_schema_name = f"{catalog_name}.{schema_name}"
        logger.info("Dropping schema: %s", full_schema_name)
        spark.sql(f"DROP SCHEMA IF EXISTS {full_schema_name} CASCADE")

    # Attach cleanup method to the function
    _create_schema.cleanup = cleanup  # type: ignore[attr-defined]

    return _create_schema


@pytest.fixture
def create_volume(spark: SparkSession, logger: logging.Logger) -> Any:
    """Fixture to create and drop a volume in Unity Catalog."""

    def _create_volume(catalog_name: str, schema_name: str, volume_name: str) -> str:
        full_volume_name = f"{catalog_name}.{schema_name}.{volume_name}"
        logger.info("Creating volume: %s", full_volume_name)

        # Create the volume
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {full_volume_name} COMMENT 'Data Platform Test Volume created at time \
                {datetime.now()}'"
        )
        return full_volume_name

    # Cleanup logic within the fixture
    def cleanup(catalog_name: str, schema_name: str, volume_name: str) -> None:
        full_volume_name = f"{catalog_name}.{schema_name}.{volume_name}"
        logger.info("Dropping volume: %s", full_volume_name)
        spark.sql(f"DROP VOLUME IF EXISTS {full_volume_name}")

    # Attach cleanup method to the function
    _create_volume.cleanup = cleanup  # type: ignore[attr-defined]

    return _create_volume


@pytest.fixture
def create_table_from_dataframe(spark: SparkSession, logger: logging.Logger, id_generator: str) -> Any:
    """Fixture to create and drop a table from a specified DataFrame."""

    def _create_table(dataframe: DataFrame, catalog_name: str, schema_name: str, table_name: str = "") -> str:
        if not table_name:
            table_name = f"{id_generator}_test_table"
        fully_qualified_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        logger.info("Creating table: %s with %d rows", fully_qualified_table_name, dataframe.count())

        # Create the table
        dataframe.write.format("delta").saveAsTable(name=fully_qualified_table_name, mode="overwrite")
        return fully_qualified_table_name

    return _create_table


@pytest.fixture(scope="session")
def copy_file_to_uc_volume(spark: SparkSession, root_dir: str) -> Any:
    """Fixture to copy a file to a Unity Catalog volume with cleanup support."""

    class CopyToUCVolume:
        # dbutils = get_dbutils(spark)
        def __call__(self, relative_config_path: str, catalog_name: str, schema_name: str, volume_name: str) -> str:
            source_path = Path(f"{root_dir}/{relative_config_path}")
            if not source_path.exists():
                raise FileNotFoundError(f"Source file '{source_path}' does not exist.")

            uc_volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{source_path.name}"

            dbutils = get_dbutils(spark)
            try:
                dbutils.fs.cp(f"file://{source_path}", uc_volume_path)  # type: ignore
            except Exception as e:
                raise ValueError(f"Failed to copy file '{source_path}': {e}") from e

            self.uc_volume_path = uc_volume_path  # Store path for cleanup
            return uc_volume_path

        def cleanup(self) -> None:
            dbutils = get_dbutils(spark)
            try:
                dbutils.fs.rm(self.uc_volume_path)  # type: ignore
            except Exception as e:
                raise ValueError(f"Failed to delete file '{self.uc_volume_path}': {e}") from e

    return CopyToUCVolume()
