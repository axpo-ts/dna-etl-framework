import logging
from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.etl.transform.transform import (
    add_audit_cols_snake_case,
    add_iso_duration_to_df,
    alias_dataframe,
    camel_case_cols,
    cast_dataframe_columns,
    cast_date_columns,
    col_rename,
    convert_columns_type,
    drop_cols,
    drop_primary_key_duplicates,
    explode_rows,
    filter,
    flatten_content,
    generate_constraints,
    generate_surrogate_hash_key,
    lower_case_columns,
    map_column_values,
    order_cols,
    remove_quotes_from_column_name,
    replace_col_values,
    safe_decimal_to_integer_conversion,
    sanitize_column_name_neat,
    sanitize_columns,
    select_cols,
    select_expr_cols,
    snake_case_columns,
    unpivot_dataframe,
    with_columns,
    zip_nested_arrays,
)


def test_select_cols(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing select cols func")
    df = spark.range(10)
    df = select_cols(df, logger, "id")
    assert df.count() == 10


def test_explode_rows(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing explode rows func")
    df = spark.createDataFrame([Row(a=1, intlist=[1, 2, 3], mapfield={"a": "b"})])
    df = explode_rows(df, logger, "intlist")
    assert df.count() == 3


def test_safe_decimal_to_integer_conversion(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing safe decimal to integer conversion")
    data = [
        (10, Decimal("12345678901234567890123456789012345678")),
        (20, Decimal("9876543210")),
        (30, Decimal("NaN")),
    ]
    # Define the schema
    schema = StructType([StructField("amount", IntegerType(), True), StructField("id", DecimalType(38, 0), True)])

    # Create the DataFrame
    df = spark.createDataFrame(data, schema)
    df = safe_decimal_to_integer_conversion(df, logger)
    assert df.schema["id"].dataType == LongType()
    assert df.filter("id is Null").count() == 1
    assert df.count() == 3


def test_remove_quotes_from_column_name(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing remove quotes from column name func")
    data = [(1, "Tom", 40), (2, "Nick", 40), (3, "Harry", 40)]
    columns = ['"id"', '"name"', '"age"']
    df = spark.createDataFrame(data, columns)
    df = remove_quotes_from_column_name(df, logger)
    assert df.columns == ["id", "name", "age"]


def test_cast_date_columns(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing cast _date columns func")
    data = [
        (1, "2024-12-01 22:00:00", "2024-12-31"),
        (2, "2024-11-03 11:00:00", "2024-11-30"),
        (3, "2024-11-05 12:00:00", "2024-11-30"),
    ]
    columns = ["id", "invoice_date", "due_date"]
    df = spark.createDataFrame(data, columns)
    df_cast = cast_date_columns(df, logger)
    assert df_cast.count() == df.count()
    assert df_cast.schema["invoice_date"].dataType == DateType()
    assert df_cast.schema["due_date"].dataType == DateType()


def test_drop_duplicates_on_primarykey(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing drop_primary_key_duplicates func")
    data = [
        (1, "Tom", "2024-12-01"),
        (2, "Venu", "2024-12-02"),
        (1, "Tom", "2024-12-01"),
        (3, "Cathy", "2024-12-03"),
        (2, "Venu", "2024-12-04"),
    ]
    columns = ["Id", "Name", "Created_Date"]
    df = spark.createDataFrame(data, schema=columns)
    df = drop_primary_key_duplicates(df, logger, "Id,Name")
    assert df.count() == 3


def test_flatten_content(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing select cols func")
    df = spark.createDataFrame([Row(mapfield={"content": "b"})])
    df = flatten_content(df, logger)
    assert df.collect()[0][0] == "b"


def test_add_audit_cols_snake_case(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing add audit columns snake case func")
    df = spark.createDataFrame([Row(mapfield={"content": "b"})])
    df = add_audit_cols_snake_case(df, logger)
    assert df.schema == StructType(
        [
            StructField("mapfield", MapType(StringType(), StringType(), True), True),
            StructField("created_at", TimestampType(), False),
            StructField("created_by", StringType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("updated_by", StringType(), False),
        ]
    )


def test_snake_case_columns(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing snake case columns func")
    df = spark.range(10)
    df = (
        df.withColumnRenamed("id", "ids")
        .withColumn("TEST_COLUMN", lit("test"))
        .withColumn("TESTCOLUMN", lit("test"))
        .withColumn("TestColumn", lit("test"))
        .withColumn("TestColumn_ID", lit("test"))
        .withColumn("CreatedBy", lit("test"))
    )
    df = snake_case_columns(df, logger, exception_columns=["TESTCOLUMN"], exception_expressions=["test", "id"])
    logger.info(df.columns)
    assert df.columns == ["ids", "test_c_o_l_u_m_n", "test_column", "test_column_id", "created_by"]


def test_lower_case_columns(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing lower case columns func")
    df = spark.range(10)
    df = (
        df.withColumnRenamed("id", "ids")
        .withColumn("TEST_COLUMN", lit("test"))
        .withColumn("TESTCOLUMN", lit("test"))
        .withColumn("TestColumn", lit("test"))
        .withColumn("TestColumn_ID", lit("test"))
        .withColumn("CreatedBy", lit("test"))
    )
    df = lower_case_columns(df, logger, exception_columns=["TestColumn_ID", "CreatedBy"])
    logger.info(df.columns)
    assert df.columns == ["ids", "test_column", "testcolumn", "TestColumn_ID", "CreatedBy"]


def test_camel_case(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing camel case cols func")
    df = spark.range(10)
    df = (
        df.withColumn("TEST_COLUMN", lit("test"))
        .withColumn("TEST_COLUMN_ID", lit("test"))
        .withColumn("created_by", lit("test"))
    )
    df = camel_case_cols(df, logger)
    assert df.columns == ["ID", "TestColumn", "TestColumnID", "created_by"]


def test_col_rename(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing rename cols func")

    # Test single column rename
    df = spark.range(10)
    df = col_rename(df, logger, input_col="id", output_col="_ID")
    assert df.columns == ["_ID"]

    # Test dictionary of renames
    df = spark.range(10)
    df = df.withColumn("test_col", lit("test"))
    rename_dict = '{"id": "ID", "test_col": "TEST_COL"}'
    df = col_rename(df, logger, col_rename_dict=rename_dict)
    assert df.columns == ["ID", "TEST_COL"]

    # Test invalid column
    df = spark.range(10)
    df = col_rename(df, logger, input_col="invalid_col", output_col="new_col")
    assert df.columns == ["id"]

    # Test no parameters
    df = spark.range(10)
    df = col_rename(df, logger)
    assert df.columns == ["id"]


def test_conver_col_type(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing rename cols func")
    df = spark.range(10)
    df = convert_columns_type(df, logger, input_col="id", col_type="string")
    assert df.schema["id"].dataType == StringType()


def test_zip_nested_arrays(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing zip nested arrays func")
    df = spark.createDataFrame(
        [(1, [11, 12], ["foo", "bla"])],  # the data
        "id int, row_label ARRAY<int>, row_value ARRAY<string>",  # column names and types
    )
    df = zip_nested_arrays(df, logger, output_col="test")
    df_2 = df.selectExpr("EXPLODE(test) as exploded").select("exploded.*")
    assert df.columns == ["id", "row_label", "row_value", "test"]
    assert df_2.columns == ["row_label", "row_value"]


def test_select_expr_cols(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing select expression columns func")
    df = spark.createDataFrame(
        [(1, 11, "test_1", "test_2")],  # the data
        "id int, col_1 int, col_2 string, col_3 string",  # column names and types
    )
    df = select_expr_cols(df, logger, output_cols=["id", "col_1", "CONCAT(col_2, ' ', col_3) AS col_23"])
    assert df.columns == ["id", "col_1", "col_23"]
    assert df.select("col_23").collect()[0][0] == "test_1 test_2"


def test_order_cols(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing select expression columns func with simple column list string")
    df = spark.createDataFrame(
        [(1, 11, "test_1", "test_2")],  # the data
        "id int, col_1 int, col_2 string, col_3 string",  # column names and types
    )
    df = order_cols(df, logger, output_cols=["`id` , `col_1` , `col_2` , `col_3`"])
    assert df.columns == ["id", "col_1", "col_2", "col_3"]


def test_drop_cols(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing drop columns func")
    df = spark.createDataFrame(
        [(1, 11, "test_1", "test_2")],  # the data
        "id int, col_1 int, col_2 string, col_3 string",  # column names and types
    )
    df_1 = drop_cols(df, logger, drop_cols=["col_1"])
    df_2 = drop_cols(df, logger, drop_cols=["col_1", "col_3"])
    assert df_1.columns == ["id", "col_2", "col_3"]
    assert df_2.columns == ["id", "col_2"]


def test_map_column_values(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing map column values func")
    df = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C"), (4, "D")],  # the data
        "id int, input string",  # column names and types
    )
    _mapping_dict = {"A": "label 1", "B": "label 2", "C": "label 2"}  # mapping for 'D' missing
    df_1 = map_column_values(df, logger, input_col_name="input", output_col_name="output", mapping_dict=_mapping_dict)
    df_2 = map_column_values(df, logger, input_col_name="input", output_col_name="input", mapping_dict=_mapping_dict)
    assert df_1.columns == ["id", "input", "output"]
    assert df_1.where("output = 'label 1'").count() == 1
    assert df_1.where("output = 'label 2'").count() == 2
    assert df_1.where("output = 'D'").count() == 1
    assert df_2.columns == ["id", "input"]


def test_with_columns(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing with columns func")
    df = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C"), (4, "D")],  # the data
        "id int, input string",  # column names and types
    )
    df = with_columns(df, logger, column_definitions={"new_col": "id * 2", "new_col_2": "'test'", "input": "123"})
    assert df.columns == ["id", "input", "new_col", "new_col_2"]
    assert df.where("new_col = 2").count() == 1
    assert df.where("new_col_2 = 'test'").count() == 4
    # Since the function skips existing columns, the input column should retain its original values
    assert df.where("input = 'A'").count() == 1
    assert df.where("input = 'B'").count() == 1
    assert df.where("input = 'C'").count() == 1
    assert df.where("input = 'D'").count() == 1


def test_filter(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing filter func")
    df = spark.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C"), (4, "C")],  # the data
        "id int, input string",  # column names and types
    )
    df_1 = filter(df, logger, filter_expr=" ")
    df_2 = filter(df, logger, filter_expr="input = 'A'")
    df_3 = filter(df, logger, filter_expr="input = 'C' AND id = 3")
    assert df_1.count() == 4
    assert df_2.count() == 1
    assert df_3.count() == 1


def test_alias_dataframe(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing alias dataframe func")
    df = spark.createDataFrame(
        [(1, 11, "test_1", "test_2")],  # the data
        "id int, col_1 int, col_2 string, col_3 string",  # column names and types
    )
    df = alias_dataframe(df, logger, alias_name="test")
    assert df.select("test.*").columns == ["id", "col_1", "col_2", "col_3"]


def test_sanitize_columns(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing sanitize columns func")
    df = spark.range(10)
    df = (
        df.withColumnRenamed("id", "ids")
        .withColumn("TEST COLUMN", lit("test"))
        .withColumn("TEST()=COLUMN", lit("test"))
        .withColumn(" ;{}()[]\n\t=-%/", lit("test"))
    )
    df = sanitize_columns(df, logger)
    logger.info(df.columns)
    assert df.columns == ["ids", "TEST_COLUMN", "TEST_COLUMN", "_"]


def test_sanitize_column_name_neat(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing sanitize column name func")
    assert sanitize_column_name_neat("test") == "test"
    assert sanitize_column_name_neat("a b;c{d}e(f)g\nh\ti=jk") == "a_b_c_d_e_f_g_h_i_jk"
    assert sanitize_column_name_neat("te ()st") == "te_st"


def test_generate_constraints(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing generate constraints func")

    schema = StructType(
        [
            StructField("Rule", StringType(), True),
            StructField("Column", StringType(), True),
            StructField("Constraint", StringType(), True),
        ]
    )

    data: list[tuple] = [
        ("NOT_NULL", "created_at, created_by", None),
        ("VALID_TIMESTAMP_FORMAT", "time_stamp_utc", None),
        ("INHERIT", "license", "license > 6"),
    ]

    df = spark.createDataFrame(data, schema)
    result_df = generate_constraints(df, "Column", logger)

    constraints = result_df.select("Constraint").collect()
    expected_constraints = [
        "created_at IS NOT NULL AND created_by IS NOT NULL",
        'to_timestamp(time_stamp_utc, "yyyy-MM-dd HH:mm:ss.SSS") IS NOT NULL',
        "license > 6",
    ]
    logger.info(f"expected_constraints : {constraints}")
    assert [row.Constraint for row in constraints] == expected_constraints


def test_add_iso_duration_to_df(spark: SparkSession, logger: logging.Logger) -> None:
    """Test the add_iso_duration_to_df function with a fixed ISO 8601 duration."""
    logger.info("Testing add_iso_duration_to_df function")

    # Create a sample DataFrame with datetime objects
    df = spark.createDataFrame(
        [
            (datetime(2024, 12, 30, 12, 0, 0),),
            (datetime(2023, 1, 1, 0, 0, 0),),
            (datetime(2022, 6, 15, 15, 45, 0),),
        ],
        "start_date timestamp",
    )

    # Define the fixed duration 15 minutes
    fixed_duration = "PT15M"
    result_df = add_iso_duration_to_df(
        df, logger, input_col="start_date", output_col="end_date", duration=fixed_duration
    )

    # Expected results created as datetime objects
    expected_data = [
        (datetime(2024, 12, 30, 12, 0, 0), datetime(2024, 12, 30, 12, 15, 0)),
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 0, 15, 0)),
        (datetime(2022, 6, 15, 15, 45, 0), datetime(2022, 6, 15, 16, 0, 0)),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        "start_date timestamp, end_date timestamp",
    )

    # Assertions
    result_data = [(row["start_date"], row["end_date"]) for row in result_df.collect()]
    expected_data = [(row["start_date"], row["end_date"]) for row in expected_df.collect()]
    assert len(result_data) == len(expected_data), "Row count does not match"
    for result_row, expected_row in zip(result_data, expected_data):
        assert result_row == expected_row, f"Row mismatch: {result_row} != {expected_row}"


def test_add_iso_duration_to_df_duration_column(spark: SparkSession, logger: logging.Logger) -> None:
    """Test the add_iso_duration_to_df function with a fixed ISO 8601 duration."""
    logger.info("Testing add_iso_duration_to_df function")

    # Create a sample DataFrame with datetime objects
    df = spark.createDataFrame(
        [
            (datetime(2024, 12, 30, 12, 0, 0), "PT15M"),
            (datetime(2023, 1, 1, 0, 0, 0), "PT1H"),
            (datetime(2022, 6, 15, 15, 45, 0), "P1D"),
        ],
        "start_date timestamp, duration string",
    )

    # Apply the function
    result_df = add_iso_duration_to_df(
        df, logger, input_col="start_date", output_col="end_date", duration_col="duration"
    )

    # Expected results created as datetime objects
    expected_data = [
        (datetime(2024, 12, 30, 12, 0, 0), datetime(2024, 12, 30, 12, 15, 0)),
        (datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 1, 0, 0)),
        (datetime(2022, 6, 15, 15, 45, 0), datetime(2022, 6, 16, 15, 45, 0)),
    ]
    expected_df = spark.createDataFrame(
        expected_data,
        "start_date timestamp, end_date timestamp",
    )

    # Assertions
    result_data = [(row["start_date"], row["end_date"]) for row in result_df.collect()]
    expected_data = [(row["start_date"], row["end_date"]) for row in expected_df.collect()]
    assert len(result_data) == len(expected_data), "Row count does not match"
    for result_row, expected_row in zip(result_data, expected_data):
        assert result_row == expected_row, f"Row mismatch: {result_row} != {expected_row}"


def test_unpivot_dataframe(spark: SparkSession, logger: logging.Logger) -> None:
    """Test the unpivot_dataframe function."""
    logger.info("Testing unpivot_dataframe function")

    # Create test dataframe with id columns and value columns
    input_df = spark.createDataFrame(
        [("A", "2023", 10.0, 20.0, 30.0), ("B", "2023", 15.0, 25.0, 35.0)], ["id", "year", "val1", "val2", "val3"]
    )

    # Test with explicit value columns
    result_df = unpivot_dataframe(
        input_df, logger=logger, id_col_name="id", value_col_name="value", value_cols=["val1", "val2", "val3"]
    )

    # Verify results
    expected_data = [
        ("A", "2023", "val1", 10.0),
        ("A", "2023", "val2", 20.0),
        ("A", "2023", "val3", 30.0),
        ("B", "2023", "val1", 15.0),
        ("B", "2023", "val2", 25.0),
        ("B", "2023", "val3", 35.0),
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "year", "curve_member", "value"])

    assert result_df.collect() == expected_df.collect()

    # Test with value column prefix
    result_df_prefix = unpivot_dataframe(
        input_df, logger=logger, id_col_name="id", value_col_name="value", value_col_prefix="val"
    )

    assert result_df_prefix.collect() == expected_df.collect()


def test_replace_col_values(spark: SparkSession, logger: logging.Logger) -> None:
    """Test the replace_col_values function."""
    logger.info("Testing replace_col_values function")

    # Create test dataframe
    input_df = spark.createDataFrame([("A", "old"), ("B", "new"), ("C", "old")], ["id", "status"])

    # Replace values
    result_df = replace_col_values(input_df, logger=logger, col_name="status", old_value="old", new_value="replaced")

    # Verify results
    expected_data = [("A", "replaced"), ("B", "new"), ("C", "replaced")]
    expected_df = spark.createDataFrame(expected_data, ["id", "status"])

    assert result_df.collect() == expected_df.collect()


def test_cast_dataframe_columns(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Testing cast_dataframe_columns function")

    # Create a sample DataFrame
    data = [(1, "25"), (2, "30")]
    df = spark.createDataFrame(data, ["id", "age"])

    # Define the schema as a string
    schema = "id INT, age int"

    # Call the function to cast DataFrame columns
    casted_df = cast_dataframe_columns(df, schema, logger)

    # Check if the returned DataFrame has the expected schema
    assert casted_df.columns == ["id", "age"], "Column names do not match expected schema"

    # Check the data types
    assert casted_df.schema["id"].dataType.typeName() == "integer", "Column 'id' is not of type int"
    assert casted_df.schema["age"].dataType.typeName() == "integer", "Column 'age' is not of type int"

    logger.info("cast_dataframe_columns test passed successfully.")


def test_generate_surrogate_key(spark: SparkSession, logger: logging.Logger) -> None:
    """Test the generate_surrogate_key function."""
    logger.info("Testing generate_surrogate_key function")

    # Create a simple input dataframe
    input_data = [
        ("schema1", "table1", "name1", "eval1"),
        ("schema2", "table2", "name2", "eval2"),
        ("schema3", "table3", "name3", "eval3"),
    ]
    input_df = spark.createDataFrame(input_data, ["schema", "table", "name", "evaluation"])

    # Generate surrogate key
    result_df = generate_surrogate_hash_key(
        df=input_df, logger=logger, cols=["schema", "table", "name", "evaluation"], key_col_name="table_id"
    )

    rows = result_df.collect()

    # Assert surrogate key column exists
    assert "table_id" in result_df.columns

    # Assert surrogate key is not null for all rows
    assert all(row["table_id"] is not None for row in rows)

    # Assert surrogate keys are unique for this small dataset
    keys = [row["table_id"] for row in rows]
    assert len(keys) == len(set(keys)), "Surrogate keys should be unique in test data"
