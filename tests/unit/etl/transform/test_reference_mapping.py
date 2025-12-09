"""Unit tests for reference data mapping functionality.

Tests both replace and enrich modes with various mapping configurations.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.etl.core import TaskContext
from data_platform.etl.transform.reference_mapping import (
    ApplyReferenceMappingsGroupsTask,
    ApplyReferenceMappingTask,
)


def create_mapping_dataframe(spark: SparkSession, mappings: list[Row]) -> DataFrame:
    """Create a DataFrame from mapping Row objects with explicit schema.

    Args:
        spark: SparkSession instance.
        mappings: List of mapping Row objects.

    Returns:
        DataFrame with explicit mapping schema.
    """
    schema = StructType(
        [
            StructField("mapping_id", IntegerType(), False),
            StructField("mapping_ref_group", StringType(), False),
            StructField("mapping_ref_col", StringType(), False),
            StructField("mapping_ref_value", StringType(), False),
            StructField("source_column", StringType(), True),
            StructField("source_value", StringType(), True),
            StructField("match_expression", StringType(), True),
            StructField("mode", StringType(), False),
            StructField("description", StringType(), True),
            StructField("valid_to", TimestampType(), True),
        ]
    )

    # Convert Row objects to tuples for DataFrame creation
    data = [
        (
            mapping.mapping_id,
            mapping.mapping_ref_group,
            mapping.mapping_ref_col,
            mapping.mapping_ref_value,
            mapping.source_column,
            mapping.source_value,
            mapping.match_expression,
            mapping.mode,
            mapping.description,
            mapping.valid_to,
        )
        for mapping in mappings
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_context(spark: SparkSession) -> TaskContext:
    """Create a mock TaskContext for testing.

    Args:
        spark: SparkSession fixture.

    Returns:
        Mocked TaskContext with SparkSession.
    """
    context = Mock(spec=TaskContext)
    context.spark = spark
    context.logger = Mock()
    context.full_unitycatalog_name = Mock(return_value="test_catalog.test_schema.reference_data_mapping")
    return context


@pytest.fixture
def sample_input_df(spark: SparkSession) -> DataFrame:
    """Create sample input DataFrame for testing.

    Args:
        spark: SparkSession fixture.

    Returns:
        Sample DataFrame with test data.
    """
    data = [
        ("€/MWh", "Power", "France", "It-Nord"),
        ("USD/Bbl", "Oil", "Germany", "DE-South"),
        ("EUR/MWh", "Gas", "Spain", "ES-Central"),
        ("unknown", "Coal", "Italy", "Unknown-Region"),
    ]
    columns = ["unit", "commodity", "country", "region"]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def replace_mappings() -> list[Row]:
    """Create sample replace mode mappings.

    Returns:
        List of mapping Row objects for replace mode.
    """
    return [
        Row(
            mapping_id=1,
            mapping_ref_group="unit",
            mapping_ref_col="name",
            mapping_ref_value="EUR/MWh",
            source_column="unit",
            source_value="€/MWh",
            match_expression=None,
            mode="replace",
            description="Standardize Euro/MWh",
            valid_to=None,
        ),
        Row(
            mapping_id=2,
            mapping_ref_group="unit",
            mapping_ref_col="name",
            mapping_ref_value="USD/Barrel",
            source_column="unit",
            source_value="USD/Bbl",
            match_expression=None,
            mode="replace",
            description="Standardize USD/Barrel",
            valid_to=None,
        ),
    ]


@pytest.fixture
def enrich_mappings() -> list[Row]:
    """Create sample enrich mode mappings.

    Returns:
        List of mapping Row objects for enrich mode.
    """
    return [
        Row(
            mapping_id=3,
            mapping_ref_group="countryext",
            mapping_ref_col="iso_code",
            mapping_ref_value="IT",
            source_column="region",
            source_value=None,
            match_expression="region LIKE 'It-%'",
            mode="enrich",
            description="Extract Italy from regions",
            valid_to=None,
        ),
        Row(
            mapping_id=4,
            mapping_ref_group="countryext",
            mapping_ref_col="iso_code",
            mapping_ref_value="DE",
            source_column="region",
            source_value=None,
            match_expression="region LIKE 'DE-%'",
            mode="enrich",
            description="Extract Germany from regions",
            valid_to=None,
        ),
        Row(
            mapping_id=5,
            mapping_ref_group="countryext",
            mapping_ref_col="iso_code",
            mapping_ref_value="ES",
            source_column="region",
            source_value=None,
            match_expression="region LIKE 'ES-%'",
            mode="enrich",
            description="Extract Spain from regions",
            valid_to=None,
        ),
    ]


class TestApplyReferenceMappingTask:
    """Test suite for ApplyReferenceMappingTask."""

    def test_replace_mode_simple_values(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
        replace_mappings: list[Row],
        spark: SparkSession,
    ) -> None:
        """Test replace mode with simple value mappings.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
            replace_mappings: Replace mode mapping fixtures.
            spark: SparkSession fixture.
        """
        # Setup mock mapping table
        mapping_df = create_mapping_dataframe(spark, replace_mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                # Execute task
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="unit",
                )
                result_df = task.execute(sample_input_df)

                # Verify results
                result_data = result_df.collect()

                # Check that €/MWh was replaced with EUR/MWh
                assert result_data[0]["unit"] == "EUR/MWh"
                # Check that USD/Bbl was replaced with USD/Barrel
                assert result_data[1]["unit"] == "USD/Barrel"
                # Check that already standard EUR/MWh remains unchanged
                assert result_data[2]["unit"] == "EUR/MWh"
                # Check that unknown values are preserved
                assert result_data[3]["unit"] == "unknown"

                # Verify other columns unchanged
                assert result_data[0]["commodity"] == "Power"
                assert result_data[0]["country"] == "France"

    def test_enrich_mode_with_expressions(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
        enrich_mappings: list[Row],
        spark: SparkSession,
    ) -> None:
        """Test enrich mode with expression-based mappings.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
            enrich_mappings: Enrich mode mapping fixtures.
            spark: SparkSession fixture.
        """
        # Setup mock mapping table
        mapping_df = create_mapping_dataframe(spark, enrich_mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                # Execute task
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="countryext",
                )
                result_df = task.execute(sample_input_df)

                # Verify results
                result_data = result_df.collect()

                # Check that new column was created
                assert "countryext" in result_df.columns

                # Check mappings
                assert result_data[0]["countryext"] == "IT"  # It-Nord -> IT
                assert result_data[1]["countryext"] == "DE"  # DE-South -> DE
                assert result_data[2]["countryext"] == "ES"  # ES-Central -> ES
                assert result_data[3]["countryext"] is None  # Unknown-Region -> null

                # Verify original columns unchanged
                assert result_data[0]["region"] == "It-Nord"
                assert result_data[1]["region"] == "DE-South"

    def test_mixed_mode_mappings(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
        replace_mappings: list[Row],
        enrich_mappings: list[Row],
        spark: SparkSession,
    ) -> None:
        """Test handling of mixed replace and enrich mappings.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
            replace_mappings: Replace mode mapping fixtures.
            enrich_mappings: Enrich mode mapping fixtures.
            spark: SparkSession fixture.
        """
        # Combine mappings with different modes
        all_mappings = replace_mappings + enrich_mappings

        # Test unit mappings (replace mode)
        unit_mapping_df = create_mapping_dataframe(spark, [m for m in all_mappings if m.mapping_ref_group == "unit"])

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=unit_mapping_df):
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="unit",
                )
                result_df = task.execute(sample_input_df)

                # Verify replace mode behavior
                assert result_df.collect()[0]["unit"] == "EUR/MWh"
                assert "unit" in result_df.columns

        # Test countryext mappings (enrich mode)
        country_mapping_df = create_mapping_dataframe(
            spark, [m for m in all_mappings if m.mapping_ref_group == "countryext"]
        )

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=country_mapping_df):
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="countryext",
                )
                result_df = task.execute(sample_input_df)

                # Verify enrich mode behavior
                assert "countryext" in result_df.columns
                assert result_df.collect()[0]["countryext"] == "IT"

    def test_no_applicable_mappings(
        self,
        mock_context: TaskContext,
        spark: SparkSession,
    ) -> None:
        """Test behavior when no mappings are applicable.

        Args:
            mock_context: Mock TaskContext.
            spark: SparkSession fixture.
        """
        # Create DataFrame with columns that don't match mappings
        df = spark.createDataFrame([("value1", "value2")], ["col1", "col2"])

        # Create mappings for different columns
        mappings = [
            Row(
                mapping_id=1,
                mapping_ref_group="test_group",
                mapping_ref_col="ref_col",
                mapping_ref_value="ref_value",
                source_column="different_column",
                source_value="test",
                match_expression=None,
                mode="replace",
                description="Test",
                valid_to=None,
            )
        ]
        mapping_df = create_mapping_dataframe(spark, mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="test_group",
                )
                result_df = task.execute(df)

                # DataFrame should be unchanged
                assert result_df.columns == df.columns
                assert result_df.collect() == df.collect()

    def test_rename_to_group_flag(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
        spark: SparkSession,
    ) -> None:
        """Test rename_to_group flag functionality.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
            spark: SparkSession fixture.
        """
        # Create mappings with a different group name to test renaming
        rename_mappings = [
            Row(
                mapping_id=1,
                mapping_ref_group="standard_unit",
                mapping_ref_col="name",
                mapping_ref_value="EUR/MWh",
                source_column="unit",
                source_value="€/MWh",
                match_expression=None,
                mode="replace",
                description="Standardize Euro/MWh",
                valid_to=None,
            ),
            Row(
                mapping_id=2,
                mapping_ref_group="standard_unit",
                mapping_ref_col="name",
                mapping_ref_value="USD/Barrel",
                source_column="unit",
                source_value="USD/Bbl",
                match_expression=None,
                mode="replace",
                description="Standardize USD/Barrel",
                valid_to=None,
            ),
        ]

        mapping_df = create_mapping_dataframe(spark, rename_mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="standard_unit",
                    rename_to_group=True,
                )
                result_df = task.execute(sample_input_df)

                # Column should be renamed to mapping_ref_group
                assert "standard_unit" in result_df.columns
                assert "unit" not in result_df.columns

                # Verify the mappings were applied
                result_data = result_df.collect()
                assert result_data[0]["standard_unit"] == "EUR/MWh"  # €/MWh -> EUR/MWh
                assert result_data[1]["standard_unit"] == "USD/Barrel"  # USD/Bbl -> USD/Barrel


class TestApplyReferenceMappingsGroupsTask:
    """Test suite for ApplyReferenceMappingsGroupsTask."""

    def test_multiple_groups_sequential_application(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
        replace_mappings: list[Row],
        enrich_mappings: list[Row],
        spark: SparkSession,
    ) -> None:
        """Test applying multiple mapping groups sequentially.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
            replace_mappings: Replace mode mapping fixtures.
            enrich_mappings: Enrich mode mapping fixtures.
            spark: SparkSession fixture.
        """
        # Create combined mappings DataFrame
        all_mappings = replace_mappings + enrich_mappings
        mapping_df = create_mapping_dataframe(spark, all_mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                task = ApplyReferenceMappingsGroupsTask(
                    context=mock_context,
                    mapping_ref_groups=["unit", "countryext"],
                )
                result_df = task.execute(sample_input_df)

                result_data = result_df.collect()

                # Verify unit mappings were applied (replace mode)
                assert result_data[0]["unit"] == "EUR/MWh"
                assert result_data[1]["unit"] == "USD/Barrel"

                # Verify countryext mappings were applied (enrich mode)
                assert "countryext" in result_df.columns
                assert result_data[0]["countryext"] == "IT"
                assert result_data[1]["countryext"] == "DE"

    def test_empty_groups_list(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
    ) -> None:
        """Test behavior with empty mapping groups list.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
        """
        task = ApplyReferenceMappingsGroupsTask(
            context=mock_context,
            mapping_ref_groups=[],
        )
        result_df = task.execute(sample_input_df)

        # DataFrame should be unchanged
        assert result_df.columns == sample_input_df.columns
        assert result_df.collect() == sample_input_df.collect()


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_missing_mapping_table(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
    ) -> None:
        """Test behavior when mapping table doesn't exist.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
        """
        with patch.object(mock_context.spark.catalog, "tableExists", return_value=False):
            task = ApplyReferenceMappingTask(
                context=mock_context,
                mapping_ref_group="unit",
            )
            result_df = task.execute(sample_input_df)

            # Should return input unchanged and log warning
            assert result_df.collect() == sample_input_df.collect()
            mock_context.logger.warning.assert_called()

    def test_invalid_expression_handling(
        self,
        mock_context: TaskContext,
        sample_input_df: DataFrame,
        spark: SparkSession,
    ) -> None:
        """Test handling of invalid SQL expressions.

        Args:
            mock_context: Mock TaskContext.
            sample_input_df: Sample input DataFrame.
            spark: SparkSession fixture.
        """
        invalid_mappings = [
            Row(
                mapping_id=1,
                mapping_ref_group="test",
                mapping_ref_col="test",
                mapping_ref_value="test_value",
                source_column=None,
                source_value=None,
                match_expression="INVALID SQL EXPRESSION !!@@##",
                mode="enrich",
                description="Invalid expression test",
                valid_to=None,
            )
        ]
        mapping_df = create_mapping_dataframe(spark, invalid_mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="test",
                )
                result_df = task.execute(sample_input_df)

                # Should handle error gracefully
                assert result_df.columns == sample_input_df.columns

    def test_null_values_in_source_column(
        self,
        mock_context: TaskContext,
        replace_mappings: list[Row],
        spark: SparkSession,
    ) -> None:
        """Test handling of null values in source columns.

        Args:
            mock_context: Mock TaskContext.
            replace_mappings: Replace mode mapping fixtures.
            spark: SparkSession fixture.
        """
        # Create DataFrame with null values
        df = spark.createDataFrame([("€/MWh",), (None,), ("USD/Bbl",)], ["unit"])

        mapping_df = create_mapping_dataframe(spark, replace_mappings)

        with patch.object(mock_context.spark.catalog, "tableExists", return_value=True):
            with patch.object(mock_context.spark, "table", return_value=mapping_df):
                task = ApplyReferenceMappingTask(
                    context=mock_context,
                    mapping_ref_group="unit",
                )
                result_df = task.execute(df)

                result_data = result_df.collect()

                # Verify mappings applied correctly
                assert result_data[0]["unit"] == "EUR/MWh"
                assert result_data[1]["unit"] is None  # Null preserved
                assert result_data[2]["unit"] == "USD/Barrel"
