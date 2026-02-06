"""Test issue #453: alias().cast() in withColumn raises SparkColumnNotFoundError.

F.col('y').alias('y_int').cast(IntegerType()) works in select() (fixed in #435) but
raised SparkColumnNotFoundError in withColumn(). ColumnValidator.validate_expression_columns
did not validate _original_column for aliased columns (same fix pattern as #435).

https://github.com/eddiethedean/sparkless/issues/453
"""

from tests.fixtures.spark_imports import get_spark_imports


def _row_val(row, key):
    """Get value from Row or dict."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


def test_alias_cast_withcolumn_exact_issue_453(spark, spark_backend):
    """Exact scenario from #453 - alias().cast() in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"x": 1, "y": "2"}])

    result = df.withColumn("y_as_int", F.col("y").alias("y_int").cast(T()))
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "x") == 1
    assert _row_val(rows[0], "y") == "2"
    assert _row_val(rows[0], "y_as_int") == 2


def test_alias_cast_withcolumn_multiple_columns(spark, spark_backend):
    """Multiple alias().cast() in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"a": "1", "b": "2"}])
    result = df.withColumn("a_int", F.col("a").alias("a_aliased").cast(T())).withColumn(
        "b_int", F.col("b").alias("b_aliased").cast(T())
    )
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "a_int") == 1
    assert _row_val(rows[0], "b_int") == 2


def test_alias_cast_select_still_works(spark, spark_backend):
    """alias().cast() in select - regression check (#435)."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"y": "2"}])
    result = df.select(F.col("y").alias("y_int").cast(T()))
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "y_int") == 2


def test_alias_cast_withcolumn_then_select(spark, spark_backend):
    """withColumn with alias().cast() then select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"x": 1, "y": "10"}])
    result = df.withColumn("y_int", F.col("y").alias("y_renamed").cast(T())).select(
        "x", "y_int"
    )
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "x") == 1
    assert _row_val(rows[0], "y_int") == 10
