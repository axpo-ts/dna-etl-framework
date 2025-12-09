# tests/test_generate_merge.py
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.etl.load.table_writer import generate_merge


def _read_table(spark: SparkSession, full_table_name: str) -> DataFrame:
    return spark.table(full_table_name)


def test_merge_insert_and_update_with_null_safe_join(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target has a normal PK row and a row with NULL PK
    target_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_by", StringType(), True),
            StructField("_source_file", StringType(), True),
        ]
    )
    target_data = [
        (1, "Alice", None, None, None),
        (None, "NullPK", None, None, None),
    ]
    target_df: DataFrame = spark.createDataFrame(target_data, target_schema)
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_null_safe_join")

    # Source has unchanged row for id=1 and changed row for NULL PK; audit columns missing
    source_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("_source_file", StringType(), True),
        ]
    )
    source_data = [
        (1, "Alice", "file_a"),  # no change
        (None, "NullPK changed", "file_b"),  # matches via <=> and changes name
        (2, "Bob", "file_c"),  # new row
    ]
    source_df: DataFrame = spark.createDataFrame(source_data, source_schema)

    # Merge: ignore _source_file; updated_at/updated_by defaults should apply on changed row
    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=["_source_file"],
        schema_evolution=False,
        created_columns=["created_at", "created_by"],  # (not in schemas, won't be used)
        updated_columns=["updated_at", "updated_by"],  # present in target, missing in source: use defaults
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table).orderBy("id")
    got = [(r["id"], r["name"], r["updated_at"], r["updated_by"], r["_source_file"]) for r in out.collect()]

    # id=None: updated via null-safe match; audit defaults set; _source_file ignored
    assert got[0][0] is None
    assert got[0][1] == "NullPK changed"
    assert got[0][2] is not None  # updated_at default applied
    assert got[0][3] is not None  # updated_by default applied
    assert got[0][4] is None  # _source_file ignored on UPDATE/INSERT

    # id=1: unchanged
    assert got[1][0] == 1
    assert got[1][1] == "Alice"
    assert got[1][2] is None
    assert got[1][3] is None
    assert got[1][4] is None

    # id=2: new insert
    assert got[2][0] == 2
    assert got[2][1] == "Bob"
    assert got[2][2] is None  # updated_at not set on INSERT
    assert got[2][3] is None  # updated_by not set on INSERT
    assert got[2][4] is None  # _source_file ignored on UPDATE/INSERT


def test_merge_only_updates_when_changed(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target row with existing audit
    ts0: datetime = datetime.now(UTC) - timedelta(days=1)
    target_df: DataFrame = spark.createDataFrame(
        [(1, "Alice", ts0, "user0")],
        "id int, name string, updated_at timestamp, updated_by string",
    )
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_only_when_changed")

    # Source row with identical non-audit fields; audit missing to force defaults (but no change => no update)
    source_df: DataFrame = spark.createDataFrame([(1, "Alice")], "id int, name string")

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,  # only update when changed
        ignore_columns=[],
        schema_evolution=False,
        updated_columns=["updated_at", "updated_by"],
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table)
    row = out.collect()[0]
    # unchanged row => updated_at/updated_by should remain as before (no update)
    assert row["updated_at"].replace(tzinfo=UTC) == ts0
    assert row["updated_by"] == "user0"


def test_merge_ignore_columns_excluded_on_insert_and_update(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    target_df: DataFrame = spark.createDataFrame(
        [(2, "Bob", None)],
        "id int, name string, _source_file string",
    )
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_ignore_columns")

    # Source includes _source_file but it is ignored
    source_df: DataFrame = spark.createDataFrame(
        [(3, "Charlie", "file_c"), (2, "Bob", "file_b")],
        "id int, name string, _source_file string",
    )

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=["_source_file"],
        schema_evolution=False,
        updated_columns=["updated_at", "updated_by"],  # not in target -> ignored
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table).orderBy("id")
    got = [(r["id"], r["name"], r["_source_file"]) for r in out.collect()]
    # Inserted row for id=3 should have _source_file NULL (not inserted), updated row keeps NULL
    assert got == [(2, "Bob", None), (3, "Charlie", None)]


def test_merge_inserts_created_columns_if_present_in_source(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target empty; we will insert with created_at/created_by present in the source
    target_df: DataFrame = spark.createDataFrame([], "id int, name string, created_at timestamp, created_by string")
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_insert_created")

    # Source includes created_* fields; ensure they insert
    src_rows = [(10, "NewGuy", datetime.now(UTC), "writer")]
    source_df: DataFrame = spark.createDataFrame(
        src_rows, "id int, name string, created_at timestamp, created_by string"
    )

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=[],  # DO NOT ignore created_* to allow insert
        schema_evolution=False,
        created_columns=["created_at", "created_by"],
        updated_columns=["updated_at", "updated_by"],
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table)
    row = out.collect()[0]
    assert row["id"] == 10
    assert row["name"] == "NewGuy"
    assert row["created_at"] is not None
    assert row["created_by"] == "writer"


def test_merge_schema_evolution_adds_new_column(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target lacks 'age' column
    target_df: DataFrame = spark.createDataFrame([(1, "Alice")], "id int, name string")
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_schema_evolution")

    # Source has a new column 'age'
    source_df: DataFrame = spark.createDataFrame([(1, "Alice", 31), (2, "Bob", 25)], "id int, name string, age int")

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=[],
        schema_evolution=True,  # enable per-merge schema evolution
        updated_columns=["updated_at", "updated_by"],  # not present in target/source: ignored
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table).orderBy("id")
    assert "age" in out.columns
    got = [(r["id"], r["name"], r["age"]) for r in out.collect()]
    # id=2 inserted with age; id=1 updated or left same; schema should carry age column
    assert got == [(1, "Alice", 31), (2, "Bob", 25)]


def test_merge_change_detection_with_nulls(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target with nullable 'attr' column
    target_df: DataFrame = spark.createDataFrame([(1, None), (2, "X")], "id int, attr string")
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_null_change_detection")

    # Source: id=1 attr None -> no change; id=2 attr None -> change from "X" to None
    source_df: DataFrame = spark.createDataFrame([(1, None), (2, None)], "id int, attr string")

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=[],
        schema_evolution=False,
        updated_columns=["updated_at"],  # not present: ignored unless defaults applied
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table).orderBy("id")
    got = [(r["id"], r["attr"]) for r in out.collect()]
    # id=1: None == None (null-safe equal) => no update; id=2: "X" != None => updated to None
    assert got == [(1, None), (2, None)]


def test_merge_updated_defaults_applied_when_missing_in_source(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target has updated_at/updated_by columns; we will update without providing them in source
    target_df: DataFrame = spark.createDataFrame(
        [(1, "Alice", None, None)],
        "id int, name string, updated_at timestamp, updated_by string",
    )
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_updated_defaults")

    # Source changes 'name'; updated_* missing -> defaults should apply on update
    source_df: DataFrame = spark.createDataFrame([(1, "Alice Smith")], "id int, name string")

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=[],
        schema_evolution=False,
        created_columns=["created_at", "created_by"],  # ignored for update
        updated_columns=["updated_at", "updated_by"],  # will be set via defaults
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table)
    row = out.collect()[0]
    assert row["name"] == "Alice Smith"
    assert row["updated_at"] is not None
    assert row["updated_by"] is not None


def test_merge_respects_drop_duplicates_before_merge(
    spark: SparkSession,
    create_table_from_dataframe: Callable[[DataFrame, str], str],
) -> None:
    # Target empty
    target_df: DataFrame = spark.createDataFrame([], "id int, val string")
    target_table: str = create_table_from_dataframe(target_df, table_name="merge_dedup_source")

    # Source has duplicates on PK
    source_df: DataFrame = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], "id int, val string")

    # Dedup the source first (simulate _apply_dedup usage)
    source_df = source_df.dropDuplicates(["id"])

    builder: Any = generate_merge(
        target_table_name=target_table,
        input_df=source_df,
        primary_keys=["id"],
        smart_update=True,
        ignore_columns=[],
        schema_evolution=False,
        updated_columns=["updated_at"],  # ignored (not present)
    )
    builder.execute()

    out: DataFrame = _read_table(spark, target_table).orderBy("id")
    got = [(r["id"], r["val"]) for r in out.collect()]
    assert got == [(1, "a"), (2, "b")]
