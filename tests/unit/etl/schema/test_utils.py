from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

import data_platform.etl.schema as schema_utils


@pytest.fixture(autouse=True)
def mock_list_tables(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch only spark.catalog.listTables inside schema_utils."""

    class MockTable:
        def __init__(self, name: str) -> None:
            self.name = name

    mock_tables = [
        MockTable("table1"),
        MockTable("_system_table"),
        MockTable("table2"),
        MockTable("excluded_table"),
    ]

    # Patch just the listTables method
    monkeypatch.setattr(spark.catalog, "listTables", MagicMock(return_value=mock_tables))


def test_get_tables_from_schema_default(spark: SparkSession) -> None:
    result = schema_utils.get_tables_from_schema(spark=spark, schema_name="default")
    assert set(result) == {"table1", "table2", "excluded_table"}


def test_get_tables_from_schema_include_system_tables(spark: SparkSession) -> None:
    result = schema_utils.get_tables_from_schema(spark=spark, schema_name="default", exclude_system_table=False)
    assert set(result) == {"table1", "table2", "_system_table", "excluded_table"}


def test_get_tables_from_schema_exclude_specific_tables(spark: SparkSession) -> None:
    result = schema_utils.get_tables_from_schema(spark=spark, schema_name="default", exclusion_table_list=["table2"])
    assert set(result) == {"table1", "excluded_table"}
