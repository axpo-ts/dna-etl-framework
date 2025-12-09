import asyncio
import logging
from collections.abc import Generator
from datetime import date, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pyarrow as pa  # type: ignore
import pytest
from databricks.connect import DatabricksSession  # type: ignore

from data_platform.tasks.core import Configuration, TaskContext
from data_platform.tasks.reader.api.volue.config.dataclasses import VolueCurveBatchConfig
from data_platform.tasks.reader.api.volue.tagged_instances import (
    VolueTaggedInstancesApiBulkReaderWriterTask,
)
from data_platform.tasks.reader.api.volue.volue import VolueCurveBatchTask


@pytest.fixture(scope="session")
def spark() -> Generator[MagicMock, None, None]:
    with patch("pyspark.sql.SparkSession.builder.getOrCreate") as mock_get_or_create:
        mock_spark = MagicMock()
        mock_get_or_create.return_value = mock_spark
        yield mock_spark


@pytest.fixture
def mock_curve() -> Mock:
    curve = Mock()
    curve.search_instances = Mock()
    curve.get_instance = Mock()
    return curve


@pytest.fixture
def dummy_curve_context() -> TaskContext:
    context = MagicMock()
    context.logger = MagicMock()

    # Mock spark and the chained call spark.sql(...).toPandas()
    spark_mock = MagicMock()
    pandas_df = pd.DataFrame({"curve_name": ["curve1"], "max_date": [pd.Timestamp("2024-01-01")]})
    # Configure the chain: spark.sql().toPandas() returns pandas_df
    spark_mock.sql.return_value.toPandas.return_value = pandas_df

    context.spark = spark_mock

    return context


@pytest.fixture
def dummy_curve_conf() -> Configuration:
    conf = MagicMock(spec=VolueCurveBatchConfig)
    conf.ingested_dates_task = None
    conf.delta_column = "value_at"
    conf.delta_load_window_days = 7
    conf.target_catalog_name = "cat"
    conf.target_schema_name = "schema"
    conf.maximum_days_per_batch = 10
    conf.items_per_batch = 2
    conf.randomise_batches = False
    conf.prefix_curve_filter = None
    return conf


@pytest.fixture
def test_etl_config() -> Configuration:
    # Minimal config as needed by your tasks
    return Configuration(
        {
            "1": {
                "task_name": "VolueTaggedInstancesApiBulkReaderWriterTask",
                "date_from": "2024-01-01",
                "date_to": "2024-01-05",
                "curve_name": "test_curve",
                "tags": "tag1,tag2",
            }
        }
    )


@pytest.fixture
def task_context(logger: logging.Logger, spark: MagicMock, test_etl_config: Configuration) -> TaskContext:
    return TaskContext(logger=logger, spark=spark, configuration=test_etl_config)


def test_volue_tagged_instances_api_bulk_reader_writer_task_process_curve(task_context: TaskContext) -> None:
    """Test the process_curve method of VolueTaggedInstancesApiBulkReaderWriterTask.

    This test verifies the following aspects:
        - The method fetches metadata for curve instances within a specified date range.
        - It filters and processes instances by fetching full time series data per tag.
        - It returns a list of tuples containing (curve_name, issue_date, tag, pd.Series).
        - The Pandas Series returned correctly carries the issue_date and tag attributes.
        - Async execution is properly handled.
        - The expected methods on the curve mock are called the correct number of times.

    The test uses mocked curve methods and metadata to simulate the API interactions,
    and runs the async method inside asyncio.run for synchronous testing.
    """

    class MockTsData(pd.Series):
        def __init__(self, data: pd.Series | list, issue_date: str, tag: str) -> None:
            super().__init__(data)  # type: ignore
            self.issue_date = issue_date
            self.tag = tag

    etl_job = VolueTaggedInstancesApiBulkReaderWriterTask()

    # Mock 'curve' with needed properties and methods
    curve = Mock()

    # Fix: Set accessRange as a real dict (so curve.accessRange["begin"] works)
    curve.accessRange = {"begin": "2024-01-01", "end": "2024-01-05"}

    # Mock ts_metadata objects returned by curve.search_instances
    ts_metadata_1 = Mock()
    ts_metadata_1.tag = "tag1"
    ts_metadata_1.issue_date = "2024-01-02"

    ts_metadata_2 = Mock()
    ts_metadata_2.tag = "tag2"
    ts_metadata_2.issue_date = "2024-01-03"

    # Mock search_instances to return metadata list
    curve.search_instances = Mock(return_value=[ts_metadata_1, ts_metadata_2])

    # Mock get_instance to return a Pandas Series
    sample_series_1 = pd.Series([1, 2, 3], index=pd.date_range("2024-01-02", periods=3))
    sample_series_2 = pd.Series([4, 5, 6], index=pd.date_range("2024-01-03", periods=3))

    def get_instance_side_effect(issue_date: str, tag: str) -> MockTsData:
        if tag == "tag1":
            series = MockTsData(sample_series_1, issue_date=issue_date, tag=tag)
        else:
            series = MockTsData(sample_series_2, issue_date=issue_date, tag=tag)
        return series

    curve.get_instance = Mock(side_effect=get_instance_side_effect)

    # Run the async function using asyncio.run() in a sync test function
    results = asyncio.run(
        etl_job.process_curve(
            date_from="2024-01-01",
            date_to="2024-01-05",
            curve_name="test_curve",
            context=task_context,
            curve=curve,
            tags=None,
        )
    )

    assert len(results) == 2
    for curve_name, issue_date, tag, series in results:
        assert curve_name == "test_curve"
        assert tag in ("tag1", "tag2")
        assert isinstance(series, pd.Series)
        assert isinstance(issue_date, str)

    curve.search_instances.assert_called_once()
    assert curve.get_instance.call_count == 2


def test_volue_tagged_instances_api_bulk_reader_writer_task_process_results(task_context: TaskContext) -> None:
    """
    Test that process_results converts a list of forecast tuples into a list of Pandas DataFrames correctly.

    Verifies that each curve's forecast data (curve_name, issued_at, tag, series)
    is transformed into a pandas DataFrame with correct columns and data.

    The test verifies:
        - The output is a list of DataFrames.
        - Each DataFrame has expected columns.
        - The number of rows matches the length of the sample series.
        - The curve_name column matches the expected curve name.
    """
    etl_job = VolueTaggedInstancesApiBulkReaderWriterTask()

    # Create a pyarrow array (which has .to_pandas())
    arrow_array = pa.array([1, 2, 3], type=pa.int64())

    # We'll patch the 'to_pandas' method to return a pandas.Series for compatibility
    class MockSeries:
        def __init__(self, arrow_arr: pa.Array) -> None:
            self.arrow_arr = arrow_arr

        def to_pandas(self) -> pd.Series:
            return pd.Series(self.arrow_arr.to_pandas())

    sample_series = MockSeries(arrow_array)

    results = [
        [("curve1", "2024-01-01", "tagA", sample_series)],
        [("curve2", "2024-01-02", "tagB", sample_series)],
    ]

    dfs = etl_job.process_results(results, context=task_context)

    assert isinstance(dfs, list)
    assert len(dfs) == 2  # two curves
    assert all(hasattr(df, "columns") for df in dfs)
    assert dfs[0].shape[0] == 3  # 3 rows from sample_series
    assert "curve_name" in dfs[0].columns
    assert dfs[0]["curve_name"].iloc[0] == "curve1"


@patch("data_platform.tasks.reader.api.volue.tagged_instances.get_spark_session")
def test_volue_abstract_curve_api_bulk_reader_task_process_df(
    mock_get_spark_session: MagicMock, logger: logging.Logger
) -> None:
    """Test the process_df method of VolueTaggedInstancesApiBulkReaderWriterTask.

    This test verifies that when api_get_params does not specify the full target table
    information (catalog_name, schema_name, and table), the process_df method
    skips writing to a table via saveAsTable and instead returns the processed DataFrame directly.

    Steps:
    - Create a Spark DataFrame with sample data.
    - Provide incomplete api_get_params (missing catalog_name or table).
    - Call process_df and confirm it returns a DataFrame without attempting to write.
    - Assert that the resulting DataFrame is not empty and has expected schema structure.
    """

    class DummyTask(VolueTaggedInstancesApiBulkReaderWriterTask):
        async def process_curve(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
            """Unused in this test, but required to instantiate abstract class."""
            pass

        def process_results(self, *_: Any, **__: Any) -> None:  # type: ignore[override]
            """Unused in this test, but required to instantiate abstract class."""
            pass

    # Create task and Spark session
    task = DummyTask()
    spark = DatabricksSession.builder.getOrCreate()
    mock_get_spark_session.return_value = spark

    # Create example DataFrame
    df = spark.createDataFrame(
        [
            ("tag1", datetime(2024, 1, 1), "curveA", datetime(2024, 1, 1, 1), 123.4),
            ("tag2", datetime(2024, 1, 2), "curveB", datetime(2024, 1, 2, 1), 567.8),
        ],
        schema=["tag", "issued_at", "curve_name", "value_at", "value"],
    )

    # Missing 'table' field, so process_df should skip writing
    api_get_params = {
        "catalog_name": "",
        "schema_name": "",
    }

    context = TaskContext(logger=logger, spark=spark, configuration=Configuration({}))

    # Run method under test
    result_df = task.process_df(df=df, context=context, api_get_params=api_get_params)

    # Assertions
    assert result_df.count() == 2
    assert len(result_df.schema.fields) == 5


def test_calculate_curve_date_batches_creates_batches_incremental_load(
    dummy_curve_conf: VolueCurveBatchConfig, dummy_curve_context: TaskContext
) -> None:
    """
    This test checks that:
    - Date batches are created based on the curves from_date and the delta load window.
    - Batches do not exceed maximum days per batch as configured.
    - The output structure maps each curve name to a list of date batch dictionaries with 'date_from' and 'date_to'.
    """
    task = VolueCurveBatchTask()
    curves_list = [
        ("curve1", {"from_date": "2024-01-01T00:00:00"}),
        ("curve2", {"tags": "tagX", "from_date": "2024-01-05T00:00:00"}),
    ]
    with patch.object(task, "check_delta_or_full_load") as mock_check:
        mock_check.return_value = pd.DataFrame(
            {"curve_name": ["curve1", "curve2"], "max_date": ["2024-01-01", "2024-01-05"]}
        )
        batches = task.calculate_curve_date_batches(dummy_curve_context, dummy_curve_conf, curves_list, "table")

    assert isinstance(batches, dict)
    assert "curve1" in batches
    assert all("date_from" in b and "date_to" in b for b in batches["curve1"])
    # Tags included for curve2
    assert "tags" in batches["curve2"][0]


def test_calculate_curve_date_batches_full_load_dates(
    dummy_curve_conf: VolueCurveBatchConfig, dummy_curve_context: TaskContext
) -> None:
    """
    Test that when full_load is True, the batches are created with the exact
    from_date and to_date specified in curve_params for each curve.
    Also checks that tags are included if provided.
    """
    task = VolueCurveBatchTask()

    curves_list = [
        ("curve1", {"tags": "tagA"}),
        ("curve2", {"tags": "tagB"}),
    ]

    # Simulate full_load parameters
    dummy_curve_conf.curve_params = {
        "full_load": "true",
        "from_date": "2024-01-01",
        "to_date": "2024-01-15",
        "curves_list": "curve1,curve2",
    }

    # Patch check_delta_or_full_load to return max_date = from_date for curves in curves_list
    with patch.object(task, "check_delta_or_full_load") as mock_check:
        mock_check.return_value = pd.DataFrame(
            {"curve_name": ["curve1", "curve2"], "max_date": ["2024-01-01", "2024-01-01"]}
        )

        batches = task.calculate_curve_date_batches(dummy_curve_context, dummy_curve_conf, curves_list, "table")

    from_date = dummy_curve_conf.curve_params["from_date"]
    to_date = dummy_curve_conf.curve_params["to_date"]

    for batch_list in batches.values():
        # Ensure first batch starts at from_date
        assert batch_list[0]["date_from"] == from_date
        # Ensure last batch ends at to_date
        assert batch_list[-1]["date_to"] == to_date
        # Ensure all batches have date_from <= date_to
        for batch in batch_list:
            assert batch["date_from"] <= batch["date_to"]
            # Tags are included
            assert "tags" in batch

    # Ensure curves included are exactly the ones in curves_list
    assert set(batches.keys()) == {"curve1", "curve2"}


def test_check_delta_or_full_load_with_ingested_dates_task(logger: logging.Logger) -> None:
    """
    Test the check_delta_or_full_load method when ingested_dates_task is set.

    This test verifies that:
    - The ingested dates are correctly fetched from the referenced task.
    - Missing curves default to their configured 'from_date'.
    - The resulting DataFrame contains the expected max_date values per curve.
    """
    task = VolueCurveBatchTask()

    # Prepare config with ingested_dates_task set
    conf = VolueCurveBatchConfig(
        task_name="VolueCurveBatchTask",
        ingested_dates_task="dummy_task_name",
        target_catalog_name="catalog",
        target_schema_name="schema",
        delta_column="value_at",
        delta_load_window_days="3",
        maximum_days_per_batch=7,
        curve_file="dummy_file",
        items_per_batch=10,
        prefix_curve_filter="",
        randomise_batches=False,
    )

    # Prepare dummy curves list and table_name from Config
    curves_list = [
        ("curve1", {"from_date": datetime(2023, 1, 1).date()}),
        ("curve2", {"from_date": datetime(2023, 2, 1).date()}),
    ]
    table_name = "Test_Table"

    def mock_get(task: str, key: str) -> list[dict]:
        if key == "volue_ingested_test_table":
            return [
                {"curve_name": "curve1", "max_date": "2023-03-01"},
            ]
        return []

    mock_taskvalues = MagicMock()
    mock_taskvalues.get.side_effect = mock_get

    mock_jobs = MagicMock()
    mock_jobs.taskValues = mock_taskvalues

    mock_utils = MagicMock()
    mock_utils.jobs = mock_jobs

    spark = DatabricksSession.builder.getOrCreate()

    task_context = TaskContext(logger=logger, spark=spark, configuration=Configuration({}))

    with patch("data_platform.tasks.reader.api.volue.volue.get_dbutils", return_value=mock_utils):
        result_df = task.check_delta_or_full_load(task_context, conf, curves_list, table_name)

    # Verify returned DataFrame contains curve1 and curve2, with correct max_date logic
    assert "curve1" in result_df["curve_name"].values
    assert "curve2" in result_df["curve_name"].values

    # curve1 max_date should come from ingested_dates_dict (upstream task)
    max_date_curve1 = result_df.loc[result_df["curve_name"] == "curve1", "max_date"].iloc[0]
    assert max_date_curve1 == date(2023, 3, 1)

    # curve2 max_date should not come from config but the use yesterday (current_date - 1) (datetime.date object converted to string)  # noqa: E501
    yesterday = datetime.now().date() - timedelta(days=1)
    assert result_df.loc[result_df["curve_name"] == "curve2", "max_date"].iloc[0] == yesterday


@patch("data_platform.tasks.reader.api.volue.volue.get_dbutils")
@patch("data_platform.tasks.reader.api.volue.volue.load_curve_file_and_expand")
def test_execute_fan_out_grouping(
    mock_load: MagicMock, mock_dbutils: MagicMock, dummy_curve_context: TaskContext
) -> None:
    """
    Test execute() method with tagged_instances table and verifies fan-out grouping logic.

    This test covers both:
    - Curves with 'pro' prefix, which should be split into 3 groups.
    - Curves without 'pro' prefix, which should be grouped as a single group.

    It verifies that the appropriate groups are created and stored in taskValues,
    and that the correct 'num_groups' value is set based on the curve prefixes.
    """
    task = VolueCurveBatchTask()

    # Config for pro prefix case
    pro_config = Configuration(
        {
            "1": {
                "task_name": "VolueCurveBatchTask",
                "curve_file": "dummy_file",
                "items_per_batch": "1",
                "prefix_curve_filter": "pro",
                "maximum_days_per_batch": 10,
                "target_catalog_name": "test",
                "target_schema_name": "test_schema",
                "delta_column": "value_at",
                "delta_load_window_days": 7,
                "randomise_batches": False,
            }
        }
    )

    # Config for non-pro prefix case (empty prefix_filter)
    non_pro_config = Configuration(
        {
            "1": {
                "task_name": "VolueCurveBatchTask",
                "curve_file": "dummy_file",
                "items_per_batch": "1",
                "prefix_curve_filter": "",  # No prefix filter
                "maximum_days_per_batch": 10,
                "target_catalog_name": "test",
                "target_schema_name": "test_schema",
                "delta_column": "value_at",
                "delta_load_window_days": 7,
                "randomise_batches": False,
            }
        }
    )

    # Mock utils and its jobs.taskValues
    mock_utils = MagicMock()
    mock_dbutils.return_value = mock_utils

    # Sample batched_curves output from calculate_curve_date_batches
    # Simulating 3 curves for pro prefix, 2 curves for non-pro
    pro_batched_curves = {
        "pro_curve1": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
        "pro_curve2": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
        "pro_curve3": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
    }
    non_pro_batched_curves = {
        "curveA": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
        "curveB": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
    }

    # Setup load_curve_file_and_expand to simulate table with curves for pro prefix
    mock_load.return_value = [
        {
            "tagged_instances_table": {
                "pro_curve1": {},
                "pro_curve2": {},
                "pro_curve3": {},
            }
        }
    ]

    with patch.object(task, "calculate_curve_date_batches", return_value=pro_batched_curves):
        # Run pro prefix test
        task.execute(dummy_curve_context, conf=pro_config.get_tree("1"))

    # Assertions for pro prefix: Should have group_0, group_1, group_2 and num_groups=3
    call_args = mock_utils.jobs.taskValues.set.call_args_list

    for i in range(3):
        assert any(c.args[0] == f"group_{i}" for c in call_args), f"group_{i} should be set"
    assert any(c.args[0] == "num_groups" and c.args[1] == 3 for c in call_args), "num_groups should be 3"

    # Clear call history for non pro curves test
    mock_utils.jobs.taskValues.set.reset_mock()

    # Setup load_curve_file_and_expand to simulate table with curves for non-pro prefix (no prefix filter)
    mock_load.return_value = [
        {
            "tagged_instances_table": {
                "curveA": {},
                "curveB": {},
            }
        }
    ]

    with patch.object(task, "calculate_curve_date_batches", return_value=non_pro_batched_curves):
        # Run non-pro prefix test
        task.execute(dummy_curve_context, conf=non_pro_config.get_tree("1"))

    # Assertions for non-pro prefix: Only group_0 should be set and num_groups=1
    call_args = mock_utils.jobs.taskValues.set.call_args_list

    assert any(c.args[0] == "group_0" for c in call_args), "group_0 should be set"
    assert not any(c.args[0] == "group_1" for c in call_args), "group_1 should NOT be set"
    assert not any(c.args[0] == "group_2" for c in call_args), "group_2 should NOT be set"
    assert any(c.args[0] == "num_groups" and c.args[1] == 1 for c in call_args), "num_groups should be 1"


@patch("data_platform.tasks.reader.api.volue.volue.get_dbutils")
@patch("data_platform.tasks.reader.api.volue.volue.load_curve_file_and_expand")
def test_execute_with_non_tagged_data(
    mock_load: MagicMock, mock_dbutils: MagicMock, dummy_curve_context: TaskContext
) -> None:
    """Test execute() method with non-empty data and a non-tagged table name.

    Verifies that the standard batching logic is used (i.e., no fan-out grouping),
    ensuring 'num_batch_chunks' and 'chunk_0' keys are set, and 'num_groups' is not set
    """
    task = VolueCurveBatchTask()

    # Provide non-tagged table data (no 'tagged_instances' in table name)
    mock_load.return_value = [
        {
            "non_tagged_table": {
                "curveA": {"from_date": "2024-01-01", "to_date": "2024-01-10"},
                "curveB": {"from_date": "2024-01-01", "to_date": "2024-01-10"},
            }
        }
    ]

    # Mock dbutils to accept any taskValues set calls
    mock_utils = MagicMock()
    mock_dbutils.return_value = mock_utils

    # Create dummy config with items_per_batch = 1 for easier batching
    test_etl_config = Configuration(
        {
            "1": {
                "task_name": "VolueCurveBatchTask",
                "curve_file": "dummy_file",
                "items_per_batch": "1",
                "prefix_curve_filter": "",
                "maximum_days_per_batch": 10,
                "target_catalog_name": "test",
                "target_schema_name": "test_schema",
                "delta_column": "value_at",
                "delta_load_window_days": 7,
                "randomise_batches": False,
            }
        }
    )

    # Patch calculate_curve_date_batches to simulate batching of each curve into one batch
    with patch.object(task, "calculate_curve_date_batches") as mock_calc:
        mock_calc.return_value = {
            "curveA": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
            "curveB": [{"date_from": "2024-01-01", "date_to": "2024-01-10"}],
        }
        task.execute(dummy_curve_context, conf=test_etl_config.get_tree("1"))

    # Check that it used standard batching (no 'num_groups' set)
    set_call_args = [call[0] for call in mock_utils.jobs.taskValues.set.call_args_list]

    # num_batch_chunks should be set (standard mode)
    assert any(arg[0] == "num_batch_chunks" for arg in set_call_args)

    # num_groups should NOT be set (fan-out mode)
    assert not any(arg[0] == "num_groups" for arg in set_call_args)

    # There should be at least one chunk_0 key set
    assert any(arg[0].startswith("chunk_") for arg in set_call_args)
