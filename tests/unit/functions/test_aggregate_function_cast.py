"""Test AggregateFunction.cast() method."""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F
from sparkless.spark_types import StringType


def test_aggregate_function_cast_method_exists():
    """Test that AggregateFunction has a cast method."""
    from sparkless.functions.base import AggregateFunction
    from sparkless.functions.core.column import Column

    col = Column("value")
    agg_func = AggregateFunction(col, "avg", None)

    # Verify cast method exists
    assert hasattr(agg_func, "cast")
    assert callable(agg_func.cast)


def test_aggregate_function_cast_returns_column_operation():
    """Test that cast() returns a ColumnOperation."""
    from sparkless.functions.base import AggregateFunction
    from sparkless.functions.core.column import Column, ColumnOperation

    col = Column("value")
    agg_func = AggregateFunction(col, "avg", None)

    # Call cast and verify it returns ColumnOperation
    result = agg_func.cast("string")
    assert isinstance(result, ColumnOperation)
    assert result.operation == "cast"
    assert result.column == agg_func
    assert result.value == "string"


def test_aggregate_function_cast_with_string_type():
    """Test cast with string type name."""
    spark = SparkSession("test")

    data = [
        {"type": "A", "value": 1},
        {"type": "A", "value": 10},
        {"type": "B", "value": 5},
    ]
    df = spark.createDataFrame(data)

    # Test cast on aggregate function
    result = df.groupby("type").agg(F.mean(F.col("value")).cast("string"))

    # Verify the result
    rows = result.collect()
    assert len(rows) == 2

    # Check that cast was applied (values should be strings)
    for row in rows:
        # Find the cast column (name may vary)
        cast_col = None
        for col_name in row.asDict():
            if "CAST" in col_name.upper() and "STRING" in col_name.upper():
                cast_col = col_name
                break

        assert cast_col is not None, "Cast column not found"
        avg_value = row[cast_col]
        assert isinstance(avg_value, str)

    spark.stop()


def test_aggregate_function_cast_with_data_type():
    """Test cast with DataType object."""
    spark = SparkSession("test")

    data = [
        {"type": "A", "value": 1},
        {"type": "A", "value": 10},
        {"type": "B", "value": 5},
    ]
    df = spark.createDataFrame(data)

    # Test cast with DataType object
    result = df.groupby("type").agg(F.mean(F.col("value")).cast(StringType()))

    rows = result.collect()
    assert len(rows) == 2

    # Check that cast was applied
    for row in rows:
        cast_col = None
        for col_name in row.asDict():
            if "CAST" in col_name.upper():
                cast_col = col_name
                break

        assert cast_col is not None
        avg_value = row[cast_col]
        assert isinstance(avg_value, str)

    spark.stop()


def test_aggregate_function_cast_with_different_aggregates():
    """Test cast with different aggregate functions."""
    spark = SparkSession("test")

    data = [
        {"type": "A", "value": 1},
        {"type": "A", "value": 10},
        {"type": "B", "value": 5},
    ]
    df = spark.createDataFrame(data)

    # Test with sum, max, min
    result = df.groupby("type").agg(
        F.sum(F.col("value")).cast("string"),
        F.max(F.col("value")).cast("int"),
        F.min(F.col("value")).cast("long"),
    )

    rows = result.collect()
    assert len(rows) == 2

    # Verify all casts were applied
    for row in rows:
        row_dict = row.asDict()
        # Check that we have cast columns
        cast_cols = [k for k in row_dict if "CAST" in k.upper()]
        assert len(cast_cols) >= 3

    spark.stop()


def test_aggregate_function_cast_column_name_format():
    """Test that cast column names follow PySpark format."""
    spark = SparkSession("test")

    data = [
        {"type": "A", "value": 1},
        {"type": "A", "value": 10},
    ]
    df = spark.createDataFrame(data)

    result = df.groupby("type").agg(F.mean(F.col("value")).cast("string"))

    rows = result.collect()
    assert len(rows) == 1

    # Check column name format: CAST(avg(value) AS STRING)
    row = rows[0]
    row_dict = row.asDict()

    # Find the cast column
    cast_col = None
    for col_name in row_dict:
        if "CAST" in col_name.upper() and "STRING" in col_name.upper():
            cast_col = col_name
            break

    assert cast_col is not None
    # Verify format matches PySpark: CAST(avg(value) AS STRING)
    assert "CAST" in cast_col
    assert "AS" in cast_col
    assert "STRING" in cast_col.upper()

    spark.stop()


def test_aggregate_function_cast_with_null_values():
    """Test cast with null values in data."""
    spark = SparkSession("test")

    data = [
        {"type": "A", "value": 1},
        {"type": "A", "value": None},
        {"type": "B", "value": 5},
    ]
    df = spark.createDataFrame(data)

    result = df.groupby("type").agg(F.mean(F.col("value")).cast("string"))

    rows = result.collect()
    assert len(rows) == 2

    # Verify results (nulls should be handled properly)
    for row in rows:
        cast_col = None
        for col_name in row.asDict():
            if "CAST" in col_name.upper():
                cast_col = col_name
                break

        if cast_col:
            value = row[cast_col]
            # Value should be either a string representation of a number or None
            assert value is None or isinstance(value, str)

    spark.stop()
