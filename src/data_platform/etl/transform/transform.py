import ast
import logging
import re
from collections.abc import Sequence
from itertools import chain
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import col, current_timestamp, current_user, explode, expr, trim, when


def explode_rows(df: DataFrame, logger: logging.Logger, explode_col: str) -> DataFrame:
    """Function to explode an arrary column in a dataframe and flatten it.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        explode_col (str): The column to explode.
    """
    return df.select(explode(explode_col).alias(f"_{explode_col}"))


def remove_quotes_from_column_name(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """Function to remove any single or double quotes in dataframe column name.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    for _col in df.columns:
        new_col_name = _col.replace("'", "").replace('"', "")
        df = df.withColumnRenamed(_col, new_col_name)
    return df


def cast_date_columns(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """Function to cast any columns having _date in their names to Date Type.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    for col_name in df.columns:
        if "_date" in col_name:
            df = df.withColumn(col_name, f.to_date(f.col(col_name)))
    return df


def safe_decimal_to_integer_conversion(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """Safely convert a DECIMAL(38,0) column to BIGINT type.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    columns_to_cast = [field.name for field in df.schema.fields if isinstance(field.dataType, t.DecimalType)]
    for _col in columns_to_cast:
        column_type = df.schema[_col].dataType
        if isinstance(column_type, t.DecimalType) and column_type.precision == 38 and column_type.scale == 0:
            df = df.withColumn(
                _col, f.when(f.col(_col).cast("bigint").isNotNull(), col(_col).cast("bigint")).otherwise(None)
            )
    return df


def convert_columns_type(df: DataFrame, logger: logging.Logger, input_col: str, col_type: str) -> DataFrame:
    """Convert the data types of specified columns in a DataFrame.

    Attributes:
        df (DataFrame): Input DataFrame.
        logger (logging.Logger): Logger for messages.
        input_col: Input Column name.
        col_type: Column type to cast.

    Returns:
        DataFrame: Updated DataFrame with converted column types.
    """
    df = df.withColumn(input_col, col(input_col).cast(col_type))
    return df


def drop_primary_key_duplicates(df: DataFrame, logger: logging.Logger, primary_key_cols: str) -> DataFrame:
    """Function to drop any duplicate rows in primary key columns.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        primary_key_cols_list (str): The of primary key columns as comma seperated values.
    """
    return df.dropDuplicates([x.strip(" ") for x in primary_key_cols.split(",")])


def select_cols(df: DataFrame, logger: logging.Logger, select_statement: str) -> DataFrame:
    """Function to pass through a specific select statement in a dataframe.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        select_statement (str): The select statement to pass through.
    """
    return df.select(select_statement)


def flatten_content(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """Function to flatten struct type columns with a column called content a dataframe.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    for col_ in df.columns:
        df = df.withColumn(col_, col(f"{col_}.content"))
    return df


def add_audit_cols_snake_case(
    df: DataFrame, logger: logging.Logger, include_cols: list[str] | None = None
) -> DataFrame:
    """Function to add audit columns to a dataframe.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        include_cols (list[str] | None): List of columns to include in the audit columns.
    """
    audit_cols = {
        "created_at": current_timestamp(),
        "created_by": current_user(),
        "updated_at": current_timestamp(),
        "updated_by": current_user(),
    }

    if include_cols is None:
        include_cols = list(audit_cols)

    # Add or overwrite only the included columns
    for _col in include_cols:
        if _col in audit_cols:
            df = df.withColumn(_col, audit_cols[_col])
        else:
            logger.warning(f"Column '{_col}' not found in audit columns. Skipping.")

    # Drop audit columns not included
    to_drop = [_col for _col in audit_cols if _col not in include_cols and _col in df.columns]
    for _col in to_drop:
        df = df.drop(_col)

    return df


def get_columns_without_audit(df: DataFrame, logger: logging.Logger) -> list[Any]:
    """Function to get columns without audit columns in a dataframe by grouping back Audit Column.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    cols_ = df.columns
    cols_without_audit = [
        col_ for col_ in cols_ if col_ not in ["created_at", "created_by", "updated_at", "updated_by"]
    ]
    return cols_without_audit


def camel_case_cols(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """Function to convert column names to camel case.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    cols_without_audit = get_columns_without_audit(df, logger)
    for col_ in cols_without_audit:
        new_col = col_.title().replace("Id", "ID").replace("_", "")
        df = df.withColumnRenamed(col_, new_col)
    return df


def lower_case_columns(df: DataFrame, logger: logging.Logger, exception_columns: list[str] = []) -> DataFrame:
    """Function to convert column names to lower case.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    logger.info(f"""Applying 'lower_case_columns' to parameters:
        \t 'exception_columns' = ' {exception_columns} '""")
    transform_columns = [col for col in df.columns if col not in exception_columns]
    for col_ in transform_columns:
        new_col = col_.lower()
        df = df.withColumnRenamed(col_, new_col)
    return df


def snake_case_columns(
    df: DataFrame, logger: logging.Logger, exception_columns: list[str] = [], exception_expressions: list[str] = []
) -> DataFrame:
    """Function to convert column names to snake case.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    logger.info(f"""Applying 'snake_case_columns' to parameters:
        \t 'exception_columns' = ' {exception_columns} '
        \t 'exception_expressions' = ' {exception_expressions} '""")
    transform_columns = [col for col in df.columns if col not in exception_columns]
    exception_expressions = ["_".join(x.lower()) for x in exception_expressions]

    logger.info(f"Applying snake case transformation to the columns: {transform_columns}")
    for col_ in transform_columns:
        new_name = re.sub(r"(?<!^)(?=[A-Z])", "_", col_).lower()
        while new_name.find("__") >= 0:
            new_name = new_name.replace("__", "_")
        # remove the snake case from exception expressions
        for _expr in exception_expressions:
            new_name = new_name.replace(_expr, _expr.replace("_", ""))
        df = df.withColumnRenamed(col_, new_name)

    return df


def col_rename(
    df: DataFrame,
    logger: logging.Logger,
    input_col: str | None = None,
    output_col: str | None = None,
    col_rename_dict: str | None = None,
) -> DataFrame:
    """Renames columns in a DataFrame based on either a single column pair or a stringified dictionary.

    Args:
        df (DataFrame): Input DataFrame.
        logger (logging.Logger): Logger used to output messages.
        input_col (str, optional): Column to rename.
        output_col (str, optional): New column name.
        col_rename_dict (str, optional): Stringified dictionary of old-to-new column names.

    Returns:
        DataFrame: The DataFrame with renamed columns.
    """
    if col_rename_dict:
        try:
            rename_dict = ast.literal_eval(col_rename_dict)
            if not isinstance(rename_dict, dict):
                raise ValueError("col_rename_dict is not a valid dictionary.")

            for old_col, new_col in rename_dict.items():
                if old_col in df.columns:
                    logger.info(f"Renaming column '{old_col}' to '{new_col}'.")
                    df = df.withColumnRenamed(old_col, new_col)
                else:
                    logger.warning(f"Column '{old_col}' not found in DataFrame.")
        except (SyntaxError, ValueError) as e:
            logger.error(f"Failed to parse col_rename_dict: {e}")
            raise
    elif input_col and output_col:
        if input_col in df.columns:
            logger.info(f"Renaming column '{input_col}' to '{output_col}'.")
            df = df.withColumnRenamed(input_col, output_col)
        else:
            logger.warning(f"Column '{input_col}' not found in DataFrame.")
    else:
        logger.warning("No valid column rename parameters provided.")

    return df


def zip_nested_arrays(df: DataFrame, logger: logging.Logger, output_col: str) -> DataFrame:
    """Function that applies 'array_zip' to array columns.

    It first identifies the array columns as any column of type 'ArrayType'. It then zips those
    arrays in the 'output_col' column.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        output_col (str): The output column containing the result.
    """
    # get the array columns
    logger.info(f"Applying 'zip_nested_arrays' to parameters:\n\t 'output_col' = ' {output_col} '")
    array_col_list = [col.name for col in df.schema if isinstance(col.dataType, t.ArrayType)]
    # Replace null array values with an array containing an empty value
    # This is to avoid null values in the zipped array
    for col_name in array_col_list:
        df = df.withColumn(col_name, f.when(f.col(col_name).isNull(), f.array(f.lit(None))).otherwise(f.col(col_name)))
    logger.info(f"In 'zip_nested_arrays':\n\t 'list of array columns' = ' {array_col_list} '")

    return df.withColumn(output_col, f.arrays_zip(*array_col_list))


def select_expr_cols(df: DataFrame, logger: logging.Logger, output_cols: list[str]) -> DataFrame:
    """Selects the requested columns including SQL expressions.

    Selects a list of columns, including new columns defined using SQL expressions.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        output_cols (list[str]): The list of output columns (including SQL expressions).
    """
    logger.info(f"Applying 'select_expr_cols' to parameters:\n\t 'output_cols' = ' {output_cols} '")
    return df.selectExpr(*output_cols)


def order_cols(df: DataFrame, logger: logging.Logger, output_cols: list[str]) -> DataFrame:
    """Selects the requested columns including SQL expressions.

    Selects a list of columns, including new columns defined using SQL expressions.

    Args:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        output_cols (list[str]): The list of output columns (including SQL expressions).
    """
    if len(output_cols) == 1 and isinstance(output_cols[0], str):
        output_cols = [col.strip() for col in output_cols[0].split(",")]

    logger.info(f"Applying 'order_cols' to parameters:\n\t output_cols = {output_cols}")
    return df.selectExpr(*output_cols)


def drop_cols(df: DataFrame, logger: logging.Logger, drop_cols: list[str]) -> DataFrame:
    """Drop columns from a dataframe.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        drop_cols (list[str]): The list of columns to drop.
    """
    logger.info(f"Applying 'drop_cols' to parameters:\n\t 'drop_cols' = ' {drop_cols} '")
    return df.drop(*drop_cols)


def map_column_values(
    df: DataFrame, logger: logging.Logger, input_col_name: str, output_col_name: str, mapping_dict: dict[Any, Any]
) -> DataFrame:
    """Create a new column whose values are given by the mapping of the input column values.

    Create a new column (or replace an existing one) by mapping the values in an existing columns
    using the dictionary provided as input. If an input values is not found in the list of dictionary
    keys, than it is passed unchanged to the output column.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        input_col_name (str): The input column names whose values should be mapped using the input dict.
        output_col_name (str): The output column containing the mapped values.
        mapping_dict (dict[]): The dictionary containing the mapping. The dict 'key' will be mapped to the dict 'value'.
    """
    logger.info(f"""Applying 'map_column_values' to parameters:
        \t 'input_col_name' = ' {input_col_name} '
        \t 'output_col_name' = ' {output_col_name} '
        \t 'mapping_dict' = ' {mapping_dict} ' """)
    # Convert each item of dictionary to map type
    mapping_expr = f.create_map([f.lit(x) for x in chain(*mapping_dict.items())])

    # Create a new column by calling the function to map the values
    return df.withColumn(output_col_name, f.coalesce(mapping_expr[f.col(input_col_name)], f.col(input_col_name)))


def with_columns(df: DataFrame, logger: logging.Logger, column_definitions: dict[str, str]) -> DataFrame:
    """Define one or more columns as SQL expressions, only if they don't already exist.

    Args:
        df (DataFrame): Input DataFrame.
        logger (logging.Logger): Logger used to output messages.
        column_definitions (dict): A dictionary where 'key = new column name' and
            'value = SQL definition of the new column'.

    Returns:
        DataFrame: DataFrame with new columns added if they did not already exist.
    """
    logger.info(f"""Applying 'with_columns' with:
        \t column_definitions = {column_definitions}""")

    existing_columns = set(df.columns)

    for col_name, sql_expression in column_definitions.items():
        if col_name not in existing_columns:
            logger.info(f"Adding new column: {col_name}")
            df = df.withColumn(col_name, f.expr(sql_expression))
        else:
            logger.info(f"Column already exists: {col_name} — skipping")

    return df


def filter(df: DataFrame, logger: logging.Logger, filter_expr: str) -> DataFrame:
    """Apply one filter provided as a SQL expression.

    Takes as input a filter and applies it to the input dataframe.
    An empty string, '', is not treated as a filter and is skipped over.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        filter_expr (str): The filter SQL expression to be applied.
    """
    logger.info(f"""Applying 'filter' to parameters:
        \t 'filter_expr' = ' {filter_expr} ' """)

    filter_expr = filter_expr.strip()
    if filter_expr != "":
        df = df.where(filter_expr)
    return df


def alias_dataframe(df: DataFrame, logger: logging.Logger, alias_name: str) -> DataFrame:
    """Gives an alias to the dataframe.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        alias_name (str): The alias name for the dataframe.
    """
    logger.info(f"Applying 'alias_dataframe' to parameters:\n\t 'alias_name' = ' {alias_name} '")
    return df.alias(alias_name)


def sanitize_columns(df: DataFrame, logger: logging.Logger) -> DataFrame:
    """For all columns in a dataframe, replace invalid characters with underscores.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
    """
    invalid_chars = [" ", ";", "{", "}", "(", ")", "[", "]", "\n", "\t", "=", "-", "%", "<", "/", "+"]
    rename_columns = df.columns
    for col_ in rename_columns:
        new_name = col_
        # replace invalid chars with _
        for char in invalid_chars:
            new_name = new_name.replace(char, "_")
        # remove multiple _ one after another
        while new_name.find("__") >= 0:
            new_name = new_name.replace("__", "_")
        df = df.withColumnRenamed(col_, new_name)
    return df


def sanitize_column_name(name: str) -> str:
    """Replace spaces and other invalid characters with underscores or other valid characters.

    Args:
        name (str): The column name to be sanitized.

    Returns:
        (str) returns the column name formated
    """
    return (
        name.replace(" ", "_")
        .replace(";", "_")
        .replace("{", "_")
        .replace("}", "_")
        .replace("[", "_")
        .replace("]", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("\n", "_")
        .replace("\t", "_")
        .replace("=", "_")
        .replace("/", "_")
        .replace("+", "_")
        .replace("-", "_")
    )


def sanitize_column_name_neat(name: str) -> str:
    """Replace spaces and other invalid characters with underscores or other valid characters.

    Args:
        name (str): The column name to be sanitized.

    Returns:
        (str) returns the column name formated
    """
    new_name = sanitize_column_name(name)
    while new_name.find("__") >= 0:
        new_name = new_name.replace("__", "_")
    return new_name


def add_iso_duration_to_df(
    df: DataFrame,
    logger: logging.Logger,
    input_col: str,
    output_col: str,
    *,
    duration: str | None = None,
    duration_col: str | None = None,
) -> DataFrame:
    """Add an ISO-8601 duration (seconds, minutes, hours, days, months or years) to df column.

    This function is designed for Databricks and uses the timestampadd function.

    Args:
        df (DataFrame): Input DataFrame.
        logger (logging.Logger): Logger for logging information.
        input_col (str): The name of the input timestamp column.
        output_col (str): The name of the output column where the result will be stored.
        duration (str, optional): An ISO-8601 duration string (e.g., "PT15M" for 15 minutes").
        duration_col (str, optional): The name of a column containing ISO-8601 duration strings.

    Returns:
        DataFrame: A new DataFrame with the output column containing the adjusted timestamps.
    """
    if bool(duration) == bool(duration_col):
        raise ValueError("Pass *either* `duration` or `duration_col`, not both / neither.")

    is_literal = duration is not None
    iso_src = f"'{duration}'" if is_literal else duration_col
    src_col = expr(iso_src) if is_literal else col(duration_col)

    def ts_add_expr(unit: str, regex: str) -> str:
        """Returns a full Spark-SQL expression."""
        value_extraction = f"CAST(regexp_extract({iso_src}, '{regex}', 1) AS INT)"
        return f"timestampadd({unit}, {value_extraction}, {input_col})"

    # Build expressions using the helper - including days
    second_expr = expr(ts_add_expr("SECOND", r"PT(\\d+)S"))
    minute_expr = expr(ts_add_expr("MINUTE", r"PT(\\d+)M"))
    hour_expr = expr(ts_add_expr("HOUR", r"PT(\\d+)H"))
    day_expr = expr(ts_add_expr("DAY", r"P(\\d+)D"))  # Added day support
    month_expr = expr(ts_add_expr("MONTH", r"P(\\d+)M"))
    year_expr = expr(ts_add_expr("YEAR", r"P(\\d+)Y"))

    duration_source_log = f'constant "{duration}"' if is_literal else f'column "{duration_col}"'
    logger.info(
        "Adding ISO-8601 duration (%s) to '%s' into new column '%s'",
        duration_source_log,
        input_col,
        output_col,
    )

    return df.withColumn(
        output_col,
        when(src_col.rlike("^PT[0-9]+S$"), second_expr)
        .when(src_col.rlike("^PT[0-9]+M$"), minute_expr)
        .when(src_col.rlike("^PT[0-9]+H$"), hour_expr)
        .when(src_col.rlike("^P[0-9]+D$"), day_expr)  # Added day pattern
        .when(src_col.rlike("^P[0-9]+M$"), month_expr)
        .when(src_col.rlike("^P[0-9]+Y$"), year_expr)
        .otherwise(None),
    )


def cast_timestamp_columns(
    df: DataFrame, logger: logging.Logger, timestamp_format: str, is_required: bool, columns: list[str] | None = None
) -> DataFrame:
    """Identify each timestamp field by data type and cast to TIMESTAMP_NTZ in a PySpark DataFrame.

    This function iterates through the DataFrame schema, identifies columns of type 'timestamp',
    and casts them to 'timestamp_ntz'. It logs the changes for each column.

    Args:
        df (DataFrame): Input DataFrame containing the timestamp columns.
        logger (logging.Logger): Logger for logging information.
        timestamp_format: The format of the timestamp columns.
        is_required (bool): Flag indicating if the casting is required.
        columns (list, optional): List of columns to cast. If None, cast all timestamp columns.

    Returns:
        DataFrame: A new DataFrame with the timestamp columns cast to 'timestamp_ntz'.
    """
    if is_required == "false":
        logger.info("Casting is not required. Skipping the for loop.")
        return df

    # If columns list is not provided, cast all timestamp columns
    if columns is None:
        columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == "timestamp"]

    # Cast specified columns to timestamp_format
    for field_name in columns:
        if dict(df.dtypes).get(field_name) == "timestamp":
            logger.info(f"Adding column {field_name} with {timestamp_format}")
            df = df.withColumn(field_name, col(field_name).cast(timestamp_format))

    return df


def generate_constraints(df: DataFrame, column_name: str, logger: logging.Logger) -> DataFrame:
    """Generates a Constraint column based on the Rule.

    Parameters:
        df (DataFrame): Input DataFrame containing rules and columns.
        column_name (str): The name of the column that contains the rules.
        logger (logging.Logger): Logger for logging information.

    Returns:
        DataFrame: Modified DataFrame with added Constraint column.
    """
    # Generate NOT NULL constraint for each column

    not_null_constraint = expr(
        f"array_join(transform(split({column_name}, ','), x -> concat(trim(x), ' IS NOT NULL')), ' AND ')"
    )

    # Generate VALID TIMESTAMP FORMAT constraint with double quotes
    timestamp_constraint = expr(
        f"array_join(transform(split({column_name}, ','), x -> "
        f"concat('to_timestamp(', x, ', \"yyyy-MM-dd HH:mm:ss.SSS\") IS NOT NULL')), ' AND ')"
    )

    constrained_df = df.withColumn(
        "Constraint",
        when(col("Constraint").isNotNull() & (trim(col("Constraint")) != ""), col("Constraint"))
        .when((f.upper(col("Rule")) == "NOT_NULL") | (f.upper(col("Rule")) == "IS_NOT_NULL"), not_null_constraint)
        .when(f.upper(col("Rule")) == "VALID_TIMESTAMP_FORMAT", timestamp_constraint)
        .otherwise(""),
    )
    logger.info("Constraints generated successfully.")
    return constrained_df


def unpivot_dataframe(
    df: DataFrame,
    logger: logging.Logger,
    id_col_name: str,
    value_col_name: str,
    value_cols: list[str] | None = None,
    value_col_prefix: str | None = None,
) -> DataFrame:
    """Unpivots a dataframe.

    Parameters:
        df (DataFrame): Input DataFrame containing the timestamp columns.
        logger (logging.Logger): Logger for logging information.
        id_col_name (str): The name of the column to use as id column.
        value_col_name (str): The name of the column to use as value column.
        value_cols (list[str]): The list of columns to use as value columns.
        value_col_prefix (str): The prefix of the columns to use as value columns.

    Returns:
        DataFrame: Unpivoted DataFrame.
    """
    if value_cols is not None:
        value_vars = value_cols
        logger.info(f"Unpivoting dataframe with value_cols: {value_cols}")
    else:
        value_vars = [col for col in df.columns if col.startswith(value_col_prefix)]  # type: ignore
        logger.info(f"Unpivoting dataframe with value_col_prefix: {value_col_prefix}")

    id_vars = [col for col in df.columns if col not in value_vars]

    # Create a list of expressions for the unpivot
    unpivot_expr = ", ".join([f"'{col}', `{col}`" for col in value_vars])
    unpivot_expr = f"stack({len(value_vars)}, {unpivot_expr}) as ({id_col_name}, {value_col_name})"

    # Apply the unpivot transformation
    unpivoted_df = df.selectExpr(*id_vars, unpivot_expr)
    logger.info(f"Unpivoted DataFrame with {len(unpivoted_df.columns)} columns")

    return unpivoted_df


def replace_col_values(
    df: DataFrame, logger: logging.Logger, col_name: str, old_value: str, new_value: str
) -> DataFrame:
    """Function to replace column values.

    Attributes:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        col_name (str): Name of the column to modify.
        old_value (str): Value to replace.
        new_value (str): New value to replace with.
    """
    return df.withColumn(col_name, f.regexp_replace(f.col(col_name).cast("string"), old_value, new_value))


def cast_dataframe_columns(
    df: DataFrame,
    schema: str,
    logger: logging.Logger,
    select_target_columns: bool = False,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Casts the columns of the input DataFrame to match the types defined in the provided schema.

    Parameters:
    df (DataFrame): The input DataFrame to be cast.
    schema (str): The spark schema but in string format.
    logger (logging.Logger): Logger for logging information.
    select_target_columns (bool): If True, selects only the columns that are present in the schema after casting.
    spark_session (SparkSession, optional): SparkSession to use. If not provided, a new SparkSession will be created.

    Returns:
    DataFrame: A new DataFrame with columns cast to the specified types.
               Columns in the schema that do not exist in the input DataFrame are ignored.
    """
    spark = spark or SparkSession.builder.getOrCreate()
    empty_df = spark.createDataFrame([], schema)

    for field in empty_df.schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))
    if select_target_columns:
        df = df.select(*empty_df.columns)

    logger.info(f"Casted Dataframe schema : {df.schema}")

    return df


def deduplicate_latest(
    df: DataFrame,
    *,
    pk_fields: Sequence[str],
    order_by_col: str,
) -> DataFrame:
    """Return one row per *pk_fields*, keeping the newest `order_by_col`."""
    w = Window.partitionBy(*pk_fields).orderBy(f.col(order_by_col).desc())
    return df.withColumn("_rn", f.row_number().over(w)).filter(f.col("_rn") == 1).drop("_rn")


def map_values(
    df: DataFrame, logger: logging.Logger, input_col_name: str, output_col_name: str, mapping_dict: dict[str, str] | str
) -> DataFrame:
    """Create a new column with mapped values using the provided mapping dictionary.

    Maps values from the input column to new values using the provided mapping dictionary.
    If an input value is not found in the mapping, it is passed unchanged to the output column.

    Args:
        df (DataFrame): Input Dataframe.
        logger (logging.Logger): Logger used to output messages.
        input_col_name (str): The input column name whose values should be mapped.
        output_col_name (str): The output column containing the mapped values.
        mapping_dict (dict[str, str] | str): Dictionary or string representation of the mapping.
    """
    if isinstance(mapping_dict, str):
        try:
            mapping_dict = ast.literal_eval(mapping_dict)
        except Exception as e:
            raise ValueError(f"Failed to parse mapping_dict string. Error: {e}")

    if not isinstance(mapping_dict, dict):
        raise ValueError(
            f"mapping_dict must be a dict or a valid string representation of a dict, got {type(mapping_dict)}"
        )

    logger.info(f"""Applying 'map_values' to parameters:
         'input_col_name' = '{input_col_name}'
         'output_col_name' = '{output_col_name}'
         'mapping_dict' = {mapping_dict}""")

    # Convert dictionary to Spark map expression
    mapping_expr = f.create_map([f.lit(x) for x in chain(*mapping_dict.items())])

    return df.withColumn(output_col_name, f.coalesce(mapping_expr[f.col(input_col_name)], f.col(input_col_name)))


def generate_surrogate_hash_key(
    df: DataFrame,
    logger: logging.Logger,
    cols: list[str],
    key_col_name: str = "abs_hash_value",
    use_timestamp: bool = False,
) -> DataFrame:
    """Function to generate a surrogate BIGINT key.

    It generates a non-deterministic absolute hash key by hashing a dynamic set of columns combined with either `UUID()`
    or the current timestamp, along with a random offset for added entropy.

    Args:
        df (DataFrame): Input DataFrame.
        logger (logging.Logger): Logger used to output messages.
        cols (List[str]): List of column names to include in the hash.
        key_col_name (str): Name of the generated surrogate key column.
        use_timestamp (bool): If True, use current_timestamp() instead of uuid()
                              to introduce uniqueness.

    Returns:
        DataFrame: DataFrame with an extra surrogate key column.
    """
    if not cols:
        raise ValueError("The list of columns for hashing cannot be empty.")

    logger.info(f"Generating surrogate key '{key_col_name}' using columns: {cols}")

    # Addind either UUID() or timestamp for uniqueness
    extra_entropy = f.expr("current_timestamp()") if use_timestamp else f.expr("uuid()")

    concat_expr = f.concat_ws("||", *[f.col(c) for c in cols], extra_entropy)

    # Hash + add randomness for uniqueness
    key_expr = (
        (f.abs(f.hash(concat_expr)).cast("bigint") * f.lit(1000000)) + (f.rand() * 999998 + 1).cast("bigint")
    ).cast("bigint")

    return df.withColumn(key_col_name, key_expr)


def add_duration_column(
    df: DataFrame,
    logger: logging.Logger,
    start_col: str,
    end_col: str,
    duration_col: str,
) -> DataFrame:
    """Add a duration column as an ISO-8601 string based on the difference between two timestamp columns.

    Args:
        df (DataFrame): Input DataFrame.
        logger (logging.Logger): Logger for logging information.
        start_col (str): The name of the start timestamp column.
        end_col (str): The name of the end timestamp column.
        duration_col (str): The name of the output column where the duration will be stored.

    Returns:
        DataFrame: A new DataFrame with the duration column added.
    """
    logger.info(
        f"Calculating duration as ISO-8601 string between '{start_col}' and '{end_col}', storing in '{duration_col}'"
    )

    # Calculate the difference in seconds
    diff_in_seconds = f.unix_timestamp(f.col(end_col)) - f.unix_timestamp(f.col(start_col))

    # Map the difference to ISO-8601 duration strings
    iso_duration_expr = (
        f.when(diff_in_seconds < 60, f.concat(f.lit("PT"), diff_in_seconds.cast("int"), f.lit("S")))
        .when(diff_in_seconds < 3600, f.concat(f.lit("PT"), (diff_in_seconds / 60).cast("int"), f.lit("M")))
        .when(diff_in_seconds < 86400, f.concat(f.lit("PT"), (diff_in_seconds / 3600).cast("int"), f.lit("H")))
        .when(diff_in_seconds < 2592000, f.concat(f.lit("P"), (diff_in_seconds / 86400).cast("int"), f.lit("D")))
        .when(diff_in_seconds < 31536000, f.concat(f.lit("P"), (diff_in_seconds / 2592000).cast("int"), f.lit("M")))
        .otherwise(f.concat(f.lit("P"), (diff_in_seconds / 31536000).cast("int"), f.lit("Y")))
    )

    return df.withColumn(duration_col, iso_duration_expr)
