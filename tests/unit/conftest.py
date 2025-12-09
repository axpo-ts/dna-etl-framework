import logging
import os
import random
import shutil
import string
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


@dataclass
class FileInfoFixture:
    """
    This class mocks the DBUtils FileInfo object
    """

    path: str
    name: str
    size: int
    modification_time: int


class DBUtilsFixture:
    """
    This class is used for mocking the behaviour of DBUtils inside tests.
    """

    def __init__(self) -> None:
        self.fs = self

    def cp(self, src: str, dest: str, recurse: bool = False) -> None:
        if recurse:
            shutil.copytree(src, dest)
        else:
            shutil.copy(src, dest)

    def ls(self, path: str) -> list[FileInfoFixture]:
        _paths = Path(path).glob("*")
        _objects = [
            FileInfoFixture(str(p.absolute()), p.name, p.stat().st_size, int(p.stat().st_mtime)) for p in _paths
        ]
        return _objects

    def mkdirs(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)

    def mv(self, src: str, dest: str, recurse: bool = False) -> None:
        copy_func = shutil.copytree if recurse else shutil.copy  # type: ignore[arg-type]
        shutil.move(src, dest, copy_function=copy_func)  # type: ignore[arg-type]

    def put(self, path: str, content: str, overwrite: bool = False) -> None:
        _f = Path(path)

        if _f.exists() and not overwrite:
            raise FileExistsError("File already exists")

        _f.write_text(content, encoding="utf-8")

    def rm(self, path: str, recurse: bool = False) -> None:
        if recurse:
            shutil.rmtree(path)
        else:
            os.remove(path)


@pytest.fixture(scope="session")
def logger() -> logging.Logger:
    return logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark(logger: logging.Logger) -> Generator[SparkSession, None, None]:
    """
    This fixture provides preconfigured SparkSession with Hive and Delta support.
    After the test session, temporary warehouse directory is deleted.
    :return: SparkSession
    """
    logger.info("Configuring Spark session for testing environment")

    spark = DatabricksSession.builder.getOrCreate()
    logger.info("Spark session configured")
    yield spark
    logger.info("Shutting down Spark session")
    spark.stop()


@pytest.fixture(scope="session", autouse=True)
def dbutils_fixture(logger: logging.Logger) -> Iterator[None]:  # type: ignore
    """
    This fixture patches the `get_dbutils` function.
    Please note that patch is applied on a string name of the function.
    If you change the name or location of it, patching won't work.
    :return:
    """
    logger.info("Patching the DBUtils object")
    with patch("data_platform.common.get_dbutils", lambda _: DBUtilsFixture()):
        yield
    logger.info("Test session finished, patching completed")


@pytest.fixture(scope="session")
def base_unit_test_path() -> Path:
    return Path(__file__).resolve().parent


# Secure random generator for unique prefixes
secure_random = random.SystemRandom()


# Function to generate random prefixes
def id_generator(size: int = 6, chars: str = string.ascii_uppercase + string.digits) -> str:
    """Generate a random prefix for schema names."""
    return "".join(secure_random.choice(chars) for _ in range(size))


@pytest.fixture(scope="session")
def test_dataframe(spark: SparkSession) -> DataFrame:
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
def create_schema(spark: SparkSession, logger: logging.Logger) -> Generator[str, None, None]:
    """Fixture to create and drop a schema."""

    prefix = id_generator()
    schema_name = f"{prefix}_unit_test_schema"
    logger.info("Creating schema: %s", schema_name)

    # Create the schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    yield schema_name

    # Cleanup: drop the schema after use
    logger.info("Dropping schema: %s", schema_name)
    spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


@pytest.fixture
def create_table_from_dataframe(spark: SparkSession, logger: logging.Logger, create_schema: str) -> Generator:
    """Fixture to create and drop a table from a specified DataFrame."""

    def _create_table(dataframe: DataFrame, table_name: str) -> str:
        """Helper function to create and return a table with the specified DataFrame and optional name."""
        if not table_name:
            prefix = id_generator()
            table_name = f"{prefix}_test_table"

        schema_qualified_table_name = f"{create_schema}.{table_name}"
        logger.info("Creating table: %s", schema_qualified_table_name)

        # Create the table
        dataframe.write.format("delta").saveAsTable(name=schema_qualified_table_name, mode="overwrite")

        return schema_qualified_table_name

    return _create_table  # type: ignore[return-value]
