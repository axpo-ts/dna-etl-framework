import glob
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.notebooks.dlt_functions import (
    create_table_ddl,
    execute_sql_script,
    get_curve_frequency,
    get_linear_interpolation,
    get_validate_dq_constraints,
    replace_parameters,
)


def test_replace_parameters() -> None:
    # Configuration
    sql_file = os.getcwd().replace("/tests", "") + "/tests/unit/files/test_replace_parameters.sql"
    params = {"source_table_name": "test"}
    expected = "SELECT * FROM test;  -- noqa\n"
    # Function execution
    actual = replace_parameters(sql_file, params)
    assert actual == expected


def test_execute_sql_script(spark: SparkSession, logger: logging.Logger) -> None:
    # Configuration
    sql_file = os.getcwd().replace("/tests", "") + "/tests/unit/files/test_execute_sql_script.sql"
    params = {"pass_value": "12345"}
    expected_count = 1
    expected_value = params["pass_value"]
    # Function execution
    actual_count = execute_sql_script(spark, sql_file, params).count()
    actual_value = execute_sql_script(spark, sql_file, params).collect()[0][0]
    assert actual_count == expected_count
    assert actual_value == expected_value


def test_get_curve_frequency(spark: SparkSession, logger: logging.Logger) -> None:
    # Configuration
    data = [("curve1", "15MIN"), ("curve2", "HOUR1"), ("curve3", "DAY1")]
    # Define schema
    schema = StructType([StructField("value_column", StringType(), True), StructField("frequency", StringType(), True)])

    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    expected = {"curve1": "15MIN", "curve2": "HOUR1", "curve3": "DAY1"}
    # Function execution
    actual = get_curve_frequency(df)
    assert actual == expected


def test_get_liner_interpolation(spark: SparkSession, logger: logging.Logger) -> None:
    # Configuration
    schema = "value_column STRING, delivery_start TIMESTAMP, frequency STRING, value DOUBLE, interpolated_value DOUBLE"
    conf = {
        "key_column_name": "value_column",
        "date_column_name": "delivery_start",
        "key_column_frequency": "frequency",
        "value_column_names": "value",
    }

    data = [
        ("A", datetime(2023, 1, 1), "D", 1.0),
        ("A", datetime(2023, 1, 2), "D", 2.0),
        ("A", datetime(2023, 1, 4), "D", 4.0),
        ("B", datetime(2023, 1, 1), "D", 10.0),
        ("B", datetime(2023, 1, 3), "D", 30.0),
    ]

    # Define schema
    source_schema = StructType(
        [
            StructField("value_column", StringType(), True),
            StructField("delivery_start", TimestampType(), True),
            StructField("frequency", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )
    df_source = spark.createDataFrame(data, source_schema)

    # Create Expected DataFrame
    expected_data = [
        {"value_column": "A", "delivery_start": datetime(2023, 1, 1), "value": 1.0, "interpolated_value": 1.0},
        {"value_column": "A", "delivery_start": datetime(2023, 1, 2), "value": 2.0, "interpolated_value": 2.0},
        {"value_column": "A", "delivery_start": datetime(2023, 1, 3), "value": None, "interpolated_value": 3.0},
        {"value_column": "A", "delivery_start": datetime(2023, 1, 4), "value": 4.0, "interpolated_value": 4.0},
        {"value_column": "B", "delivery_start": datetime(2023, 1, 1), "value": 10.0, "interpolated_value": 10.0},
        {"value_column": "B", "delivery_start": datetime(2023, 1, 2), "value": None, "interpolated_value": 20.0},
        {"value_column": "B", "delivery_start": datetime(2023, 1, 3), "value": 30.0, "interpolated_value": 30.0},
    ]
    expected = pd.DataFrame(expected_data).astype(
        {"value_column": "str", "delivery_start": "datetime64[ns]", "value": "float64", "interpolated_value": "float64"}
    )
    actual = get_linear_interpolation(df_source, conf, schema).drop("frequency").toPandas()
    pd.testing.assert_frame_equal(actual, expected)


def test_get_validate_dq_constraints(spark: SparkSession) -> None:
    """Test if Any Constraint or Expression used in file has prefix as _drop_"""

    search_path = str(Path(__file__).parents[0] / "files")
    dataframes = []
    for filepath in glob.glob(os.path.join(search_path, "**", "dq.json"), recursive=True):
        with open(filepath) as f:
            data = json.loads(f.read())
        df = spark.createDataFrame(data).withColumn("filepath", lit(filepath))
        dataframes.append(df)

    if dataframes:
        final_df = dataframes[0]  #
        for df in dataframes[1:]:
            final_df = final_df.union(df)
    else:
        final_df = spark.createDataFrame([], schema="Constraint STRING")

    # Ensure the dq file have been recognized from the search path
    assert final_df.count() > 1

    df = final_df.filter("Constraint like '%alias%'").filter("Constraint IS NOT NULL")

    df = get_validate_dq_constraints(df)

    # Use df.show() for any failures to debug to get the dq file name having issue
    assert df.filter("is_constraint_valid == False or is_aliases_valid == False").count() == 0


def test_create_table_ddl_with_schema(spark: SparkSession) -> None:
    primary_keys = "id,reference_time"
    target_table = "test_target"
    schema = """id string COMMENT 'Unique identifier', reference_time timestamp COMMENT 'The timestamp',
    delivery_start timestamp COMMENT 'delivery start time ', delivery_end timestamp COMMENT 'deliveryend time'"""
    ddl_stmt, pks = create_table_ddl(spark, primary_keys, target_table, schema=schema, add_table_id=False)

    expected = schema + ", CONSTRAINT pk_test_target PRIMARY KEY(id, reference_time)"

    assert ddl_stmt == expected
    assert pks == ["id", "reference_time"]


def test_create_table_ddl_with_table(spark: SparkSession, create_schema: str) -> None:
    primary_keys = "id,reference_time"
    target_table = "test_target"
    df = (
        spark.range(1)
        .withColumn("reference_time", lit(current_timestamp()))
        .withColumn("delivery_start", lit(current_timestamp()))
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_table_ddl")
    ddl_stmt, pks = create_table_ddl(
        spark, primary_keys, target_table, src_table=f"{create_schema}.test_table_ddl", add_table_id=False
    )

    expected = """id long, reference_time timestamp, delivery_start timestamp, CONSTRAINT pk_test_target PRIMARY KEY(id, reference_time)"""  # noqa: E501
    assert ddl_stmt == expected
    assert pks == ["id", "reference_time"]


def test_create_table_ddl_with_auto_identity_column(spark: SparkSession, create_schema: str) -> None:
    primary_keys = "id,reference_time"
    target_table = "test_target"
    df = (
        spark.range(1)
        .withColumn("reference_time", lit(current_timestamp()))
        .withColumn("delivery_start", lit(current_timestamp()))
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{create_schema}.test_table_ddl")
    ddl_stmt, pks = create_table_ddl(spark, primary_keys, target_table, src_table=f"{create_schema}.test_table_ddl")
    expected = (
        "id long, "
        "reference_time timestamp, "
        "delivery_start timestamp, "
        "table_id BIGINT not null GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1), "
        "CONSTRAINT pk_test_target PRIMARY KEY(id, reference_time)"
    )
    assert ddl_stmt.lower() == expected.lower()


def test_get_liner_interpolation_columns(spark: SparkSession, logger: logging.Logger) -> None:
    # Configuration
    schema = "value_column STRING, delivery_start TIMESTAMP, frequency STRING, value1 DOUBLE, value2 DOUBLE, interpolated_value1 DOUBLE, interpolated_value2 DOUBLE"  # noqa: E501
    conf = {
        "key_column_name": "value_column",
        "date_column_name": "delivery_start",
        "key_column_frequency": "frequency",
        "value_column_names": "value1,value2",
    }

    data = [
        ("A", datetime(2023, 1, 1), "D", 1.0, 1.0),
        ("A", datetime(2023, 1, 2), "D", 2.0, 2.0),
        ("A", datetime(2023, 1, 4), "D", 4.0, 4.0),
        ("B", datetime(2023, 1, 1), "D", 10.0, 10.0),
        ("B", datetime(2023, 1, 3), "D", 30.0, 30.0),
    ]

    # Define schema
    source_schema = StructType(
        [
            StructField("value_column", StringType(), True),
            StructField("delivery_start", TimestampType(), True),
            StructField("frequency", StringType(), True),
            StructField("value1", DoubleType(), True),
            StructField("value2", DoubleType(), True),
        ]
    )
    df_source = spark.createDataFrame(data, source_schema)

    # Create Expected DataFrame
    expected_data = [
        {
            "value_column": "A",
            "delivery_start": datetime(2023, 1, 1),
            "value1": 1.0,
            "value2": 1.0,
            "interpolated_value1": 1.0,
            "interpolated_value2": 1.0,
        },
        {
            "value_column": "A",
            "delivery_start": datetime(2023, 1, 2),
            "value1": 2.0,
            "value2": 2.0,
            "interpolated_value1": 2.0,
            "interpolated_value2": 2.0,
        },
        {
            "value_column": "A",
            "delivery_start": datetime(2023, 1, 3),
            "value1": None,
            "value2": None,
            "interpolated_value1": 3.0,
            "interpolated_value2": 3.0,
        },
        {
            "value_column": "A",
            "delivery_start": datetime(2023, 1, 4),
            "value1": 4.0,
            "value2": 4.0,
            "interpolated_value1": 4.0,
            "interpolated_value2": 4.0,
        },
        {
            "value_column": "B",
            "delivery_start": datetime(2023, 1, 1),
            "value1": 10.0,
            "value2": 10.0,
            "interpolated_value1": 10.0,
            "interpolated_value2": 10.0,
        },
        {
            "value_column": "B",
            "delivery_start": datetime(2023, 1, 2),
            "value1": None,
            "value2": None,
            "interpolated_value1": 20.0,
            "interpolated_value2": 20.0,
        },
        {
            "value_column": "B",
            "delivery_start": datetime(2023, 1, 3),
            "value1": 30.0,
            "value2": 30.0,
            "interpolated_value1": 30.0,
            "interpolated_value2": 30.0,
        },
    ]
    expected = pd.DataFrame(expected_data).astype(
        {
            "value_column": "str",
            "delivery_start": "datetime64[ns]",
            "value1": "float64",
            "value2": "float64",
            "interpolated_value1": "float64",
            "interpolated_value2": "float64",
        }
    )
    actual = get_linear_interpolation(df_source, conf, schema).drop("frequency").toPandas()
    pd.testing.assert_frame_equal(actual, expected)
