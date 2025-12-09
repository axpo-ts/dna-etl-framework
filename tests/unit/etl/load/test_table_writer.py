from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.etl.load.table_writer import (
    BatchTableAppend,
    BatchTableInsertOverwrite,
    BatchTableOverwrite,
    BatchTableUpsert,
    BatchTableWriteSCDType2,
    StreamingTableUpsert,
    StreamTableWriteSCDType2,
    _BaseStreamingTableUpsert,
    _BaseTableWriter,
)


def is_spark_dataframe(x: Any) -> bool:
    """Check if x is a Spark DataFrame (local or connect)."""
    types = []
    try:
        from pyspark.sql import DataFrame as LocalDF

        types.append(LocalDF)
    except Exception:
        pass
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDF

        types.append(ConnectDF)
    except Exception:
        pass
    return isinstance(x, tuple(types))


def _naive_utc_now() -> datetime:
    """Return naive UTC now for Spark-friendly timestamp creation."""
    return datetime.now(UTC).replace(tzinfo=None)


class DummyContext:
    """Minimal TaskContext-like stub with spark and logger attributes."""

    def __init__(self, spark: SparkSession, logger: logging.Logger) -> None:
        self.spark = spark
        self.logger = logger


class DummyIdentifier:
    """Minimal identifier stub; content unused when we override _full_unitycatalog_name_of."""

    def __init__(self, name: str) -> None:
        self.name = name


class DummyTableModel:
    """Minimal AbstractTableModel-like stub."""

    def __init__(self, primary_keys: list[str], identifier: Any) -> None:
        self.primary_keys = primary_keys
        self.identifier = identifier
        self.audit_columns: list[str] = []


class TestBaseWriter(_BaseTableWriter):
    """Concrete subclass to satisfy abstract execute() and control full table name."""

    task_name: str = "TestBaseWriter"
    _forced_table_name: str

    def __init__(
        self,
        context: DummyContext,
        table_model: DummyTableModel,
        source_df: DataFrame | None,
        primary_keys: list[str] | str,
        writer_options: dict[str, str] | None = None,
        drop_duplicates: bool = False,
        order_by_col: str = "updated_at",
        created_column: str = "created_at",
        forced_table_name: str | None = None,
    ) -> None:
        super().__init__(
            context=context,
            table_model=table_model,
            source_df=source_df,
            primary_keys=primary_keys,
            writer_options=writer_options or {},
            drop_duplicates=drop_duplicates,
            order_by_col=order_by_col,
            created_column=created_column,
        )
        self._forced_table_name = forced_table_name or "default.test_table"
        # Provide default trigger_kwargs to avoid attribute errors
        self.trigger_kwargs = {}

    def _full_unitycatalog_name_of(self, identifier: Any) -> str:
        return self._forced_table_name

    def _full_base_source_filename_of(self, _: Any) -> str:
        return "/base"

    def execute(self, source_df: DataFrame | None = None) -> None:
        # No-op for unit tests
        pass


def test_apply_dedup_latest_wins(
    spark: SparkSession,
    logger: logging.Logger,
) -> None:
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("val", StringType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )
    t1 = _naive_utc_now()
    t2 = _naive_utc_now()
    df: DataFrame = spark.createDataFrame([(1, "old", t1), (1, "new", t2), (2, "only", t1)], schema)

    writer = TestBaseWriter(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=None,
        primary_keys=["id"],
        drop_duplicates=True,
        order_by_col="updated_at",
    )
    out: DataFrame = writer._apply_dedup(df)
    count = out.groupBy("id").count().filter("count > 1").count()
    assert count == 0
    survivor_val = out.filter("id = 1").collect()[0]["val"]
    assert survivor_val == "new"
    total = out.count()
    assert total == 2


def test_apply_dedup_ties_keep_single_row(
    spark: SparkSession,
    logger: logging.Logger,
) -> None:
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("val", StringType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )
    t = _naive_utc_now()
    df: DataFrame = spark.createDataFrame([(1, "a", t), (1, "b", t), (2, "c", t)], schema)

    writer = TestBaseWriter(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=None,
        primary_keys=["id"],
        drop_duplicates=True,
        order_by_col="updated_at",
    )
    out: DataFrame = writer._apply_dedup(df)
    count = out.groupBy("id").count().filter("count > 1").count()
    assert count == 0
    total = out.count()
    assert total == 2
    val = out.filter("id = 1").collect()[0]["val"]
    assert val in {"a", "b"}


def test_apply_dedup_nulls_last(
    spark: SparkSession,
    logger: logging.Logger,
) -> None:
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("val", StringType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )
    t = _naive_utc_now()
    df: DataFrame = spark.createDataFrame([(3, "nonnull", t), (3, "null", None)], schema)

    writer = TestBaseWriter(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=None,
        primary_keys=["id"],
        drop_duplicates=True,
        order_by_col="updated_at",
    )
    out: DataFrame = writer._apply_dedup(df)
    total = out.count()
    assert total == 1
    val = out.collect()[0]["val"]
    assert val == "nonnull"


def test_apply_dedup_fallback_drop_duplicates(
    spark: SparkSession,
    logger: logging.Logger,
) -> None:
    df: DataFrame = spark.createDataFrame([(1, "x"), (1, "y"), (2, "z")], "id int, val string")

    writer = TestBaseWriter(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=None,
        primary_keys=["id"],
        drop_duplicates=True,
        order_by_col="updated_at",  # missing in df
    )
    out: DataFrame = writer._apply_dedup(df)
    total = out.count()
    assert total == 2
    count = out.groupBy("id").count().filter("count > 1").count()
    assert count == 0


def test_batch_overwrite_executes_overwrite(
    spark: SparkSession,
    logger: logging.Logger,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Create a target table
    target_df: DataFrame = spark.createDataFrame([(1, "old"), (2, "keep")], "id int, val string")
    target_name: str = create_table_from_dataframe(target_df, table_name="batch_overwrite_target")

    # Source has duplicates on PK with newer column present
    t1 = _naive_utc_now()
    t2 = _naive_utc_now()
    source_schema = "id int, val string, updated_at timestamp"
    source_df: DataFrame = spark.createDataFrame([(1, "new", t2), (1, "newer", t1), (3, "add", t2)], source_schema)

    writer = BatchTableOverwrite(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=source_df,
        primary_keys=["id"],
        drop_duplicates=False,
        overwrite_schema=True,
    )
    # Force table name resolution
    writer._full_unitycatalog_name_of = lambda _: target_name  # type: ignore[attr-defined]
    writer.drop_duplicates = False
    writer.order_by_col = "updated_at"
    writer.execute(None)

    out: DataFrame = spark.table(target_name).orderBy("id")
    total = out.count()
    assert total == 3
    val1 = out.filter("id = 1").collect()[0]["val"]
    assert val1 == "new"
    val3 = out.filter("id = 3").collect()[0]["val"]
    assert val3 == "add"


def test_batch_insert_overwrite_executes(
    spark: SparkSession,
    logger: logging.Logger,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    target_df: DataFrame = spark.createDataFrame([], "id int, val string")
    target_name: str = create_table_from_dataframe(target_df, table_name="batch_insert_overwrite_target")

    source_df: DataFrame = spark.createDataFrame([(1, "a"), (2, "b")], "id int, val string")

    writer = BatchTableInsertOverwrite(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=source_df,
        primary_keys=["id"],
    )
    writer._full_unitycatalog_name_of = lambda _: target_name  # type: ignore[attr-defined]
    writer.execute(None)

    out: DataFrame = spark.table(target_name).orderBy("id")
    total = out.count()
    assert total == 2
    val1 = out.filter("id = 1").collect()[0]["val"]
    assert val1 == "a"
    val2 = out.filter("id = 2").collect()[0]["val"]
    assert val2 == "b"


def test_batch_append_executes(
    spark: SparkSession,
    logger: logging.Logger,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    target_df: DataFrame = spark.createDataFrame([(1, "a")], "id int, val string")
    target_name: str = create_table_from_dataframe(target_df, table_name="batch_append_target")

    source_df: DataFrame = spark.createDataFrame([(2, "b"), (3, "c")], "id int, val string")

    writer = BatchTableAppend(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=source_df,
        primary_keys=["id"],
    )
    writer._full_unitycatalog_name_of = lambda _: target_name  # type: ignore[attr-defined]
    writer.execute(None)

    out: DataFrame = spark.table(target_name).orderBy("id")
    total = out.count()
    assert total == 3
    vals = [r["val"] for r in out.collect()]
    assert "b" in vals
    assert "c" in vals


def test_base_table_upsert_calls_generate_merge(
    spark: SparkSession,
    logger: logging.Logger,
    monkeypatch: Any,
) -> None:
    # Source DF
    source_df: DataFrame = spark.createDataFrame([(1, "a"), (1, "b")], "id int, val string")

    # Prepare writer
    writer = BatchTableUpsert(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=source_df,
        primary_keys=["id"],
    )
    writer._full_unitycatalog_name_of = lambda _: "default.upsert_target"  # type: ignore[attr-defined]
    writer.drop_duplicates = True
    writer.order_by_col = "updated_at"

    called: dict[str, Any] = {"count": 0, "args": None}

    class FakeBuilder:
        def execute(self) -> None:
            called["count"] += 1

    def fake_generate_merge(**kwargs: Any) -> FakeBuilder:
        called["args"] = kwargs
        return FakeBuilder()

    # Patch generate_merge in module where writer is defined
    monkeypatch.setattr("data_platform.etl.load.table_writer.generate_merge", fake_generate_merge)
    writer._run_merge(source_df)

    c = called["count"]
    assert c == 1
    args = called["args"]
    assert args["primary_keys"] == ["id"]
    assert args["target_table_name"] == "default.upsert_target"
    assert is_spark_dataframe(args["input_df"])


def test_base_streaming_table_upsert_foreach_dedup_and_merge(
    spark: SparkSession,
    logger: logging.Logger,
    monkeypatch: Any,
) -> None:
    micro_df: DataFrame = spark.createDataFrame(
        [(1, "x", _naive_utc_now()), (1, "y", _naive_utc_now()), (2, "z", _naive_utc_now())],
        "id int, val string, updated_at timestamp",
    )

    # Prepare base class and function
    base = _BaseStreamingTableUpsert()
    # Inject a context with logger for logging
    base.context = DummyContext(spark, logger)  # type: ignore[attr-defined]

    called: dict[str, Any] = {"count": 0, "df_count": None}

    class FakeBuilder:
        def execute(self) -> None:
            called["count"] += 1

    def fake_generate_merge(**kwargs: Any) -> FakeBuilder:
        called["df_count"] = kwargs["input_df"].count()
        return FakeBuilder()

    monkeypatch.setattr("data_platform.etl.load.table_writer.generate_merge", fake_generate_merge)

    foreach_fn = base._create_standalone_foreach_batch(
        table_name="default.stream_target",
        primary_keys=["id"],
        created_columns=["created_at", "created_by"],
        updated_columns=["updated_at", "updated_by"],
        ignore_columns=["table_id", "_source_file"],
        enable_schema_evolution=False,
        smart_update=True,
        order_by_col="updated_at",
    )

    foreach_fn(micro_df, 1)

    c = called["count"]
    assert c == 1
    dfc = called["df_count"]
    assert dfc == 2


def test_streaming_table_upsert_execute_calls_stream_with_foreach(
    spark: SparkSession,
    logger: logging.Logger,
    monkeypatch: Any,
) -> None:
    src: DataFrame = spark.createDataFrame([(1, "a")], "id int, val string")
    writer = StreamingTableUpsert(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=src,
        primary_keys=["id"],
    )
    writer._full_unitycatalog_name_of = lambda _: "default.stream_target"  # type: ignore[attr-defined]

    called: dict[str, Any] = {"mode": None, "foreach_present": None}

    def fake_stream(df: DataFrame, *, output_mode: str, foreach_fn: Callable[[DataFrame, int], None] | None) -> None:
        called["mode"] = output_mode
        called["foreach_present"] = foreach_fn is not None

    monkeypatch.setattr(writer, "_stream", fake_stream)
    writer.execute()

    mode = called["mode"]
    assert mode == "append"
    present = called["foreach_present"]
    assert present is True


def test_batch_scd_type2_execute_calls_run_merge(
    spark: SparkSession,
    logger: logging.Logger,
    monkeypatch: Any,
) -> None:
    src: DataFrame = spark.createDataFrame([(1, "x")], "id int, val string")
    writer = BatchTableWriteSCDType2(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=src,
        primary_keys=["id"],
    )

    called: dict[str, Any] = {"count": 0}

    def fake_run(source_df: DataFrame) -> None:
        called["count"] += 1

    monkeypatch.setattr(writer, "_run_merge_scd_type2", fake_run)
    writer.execute(None)

    c = called["count"]
    assert c == 1


def test_stream_scd_type2_execute_builds_foreach_and_runs_stream(
    spark: SparkSession,
    logger: logging.Logger,
    monkeypatch: Any,
) -> None:
    src: DataFrame = spark.createDataFrame([(1, "x")], "id int, val string")
    writer = StreamTableWriteSCDType2(
        context=DummyContext(spark, logger),
        table_model=DummyTableModel(primary_keys=["id"], identifier=DummyIdentifier("x")),
        source_df=src,
        primary_keys=["id"],
    )
    writer._full_unitycatalog_name_of = lambda _: "default.scd2_target"  # type: ignore[attr-defined]

    called: dict[str, Any] = {"foreach_called": None}

    def fake_run(source_df: DataFrame) -> None:
        called["foreach_called"] = True

    def fake_stream(df: DataFrame, *, output_mode: str, foreach_fn: Callable[[DataFrame, int], None] | None) -> None:
        # Simulate one micro-batch run
        assert foreach_fn is not None
        foreach_fn(df, 1)

    monkeypatch.setattr(writer, "_run_merge_scd_type2", fake_run)
    monkeypatch.setattr(writer, "_stream", fake_stream)
    writer.execute(None)

    called_flag = called["foreach_called"]
    assert called_flag is True
