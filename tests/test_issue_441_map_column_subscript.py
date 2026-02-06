"""Test issue #441: F.col('MapValue')[F.col('Value')] - map column subscript with Column key.

PySpark supports using a Column as the lookup key for a map column:
  F.col("MapValue")[F.col("Value")]  # lookup MapValue using key from Value column

Fixed by #440 - Column.__getitem__ now delegates to getItem for Column keys.

Run in PySpark mode first, then mock mode:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_441_map_column_subscript.py -v
  pytest tests/test_issue_441_map_column_subscript.py -v

https://github.com/eddiethedean/sparkless/issues/441
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_map_column_subscript_with_column_key_exact_issue_441(spark, spark_backend):
    """Exact scenario from #441 - map in column, lookup key from another column."""
    F = get_spark_imports(spark_backend).F

    # Use string keys for cross-backend compatibility (Polars createDataFrame + int map keys)
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "1", "MapValue": {"1": "Small", "2": "Medium"}},
            {"Name": "Bob", "Value": "2", "MapValue": {"2": "Medium", "3": "Large"}},
        ]
    )
    df = df.withColumn("Size", F.col("MapValue")[F.col("Value")])
    rows = df.collect()

    assert rows[0]["Name"] == "Alice"
    assert rows[0]["Size"] == "Small"
    assert rows[1]["Name"] == "Bob"
    assert rows[1]["Size"] == "Medium"


def test_map_column_subscript_key_not_found(spark, spark_backend):
    """map_col[key_col] returns None when key not in map."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"k": "a", "m": {"a": 1, "b": 2}},
            {"k": "z", "m": {"a": 1, "b": 2}},
        ]
    )
    df = df.withColumn("v", F.col("m")[F.col("k")])
    rows = df.collect()

    assert rows[0]["v"] == 1
    assert rows[1]["v"] is None


def test_map_column_subscript_in_select(spark, spark_backend):
    """map_col[key_col] works in select."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"id": 1, "key": "x", "data": {"x": 10, "y": 20}}])
    result = df.select(F.col("data")[F.col("key")].alias("val"))
    rows = result.collect()

    assert rows[0]["val"] == 10
