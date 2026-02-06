"""Test issue #434: LTRIM and RTRIM in F.expr().

PySpark supports F.expr("LTRIM(RTRIM(Value))") and nested function calls.
Sparkless SQLExprParser previously raised ParseException for these expressions.
Now parses LTRIM, RTRIM, and nested function calls in F.expr() for PySpark parity.

https://github.com/eddiethedean/sparkless/issues/434
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_expr_ltrim_rtrim_exact_issue_434(spark, spark_backend):
    """Exact scenario from issue #434 - LTRIM(RTRIM(Value))."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "  sales department"},
            {"Name": "Bob", "Value": "tech department  "},
            {"Name": "Charlie", "Value": " ceo "},
        ]
    )
    df = df.select(F_backend.expr("LTRIM(RTRIM(Value))"))
    df.show()
    rows = df.collect()
    assert len(rows) == 3
    col_name = "ltrim(rtrim(Value))"
    assert rows[0][col_name] == "sales department"
    assert rows[1][col_name] == "tech department"
    assert rows[2][col_name] == "ceo"


def test_expr_ltrim_only(spark, spark_backend):
    """LTRIM alone."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "  hello"}])
    result = df.select(F_backend.expr("LTRIM(s)").alias("t"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["t"] == "hello"


def test_expr_rtrim_only(spark, spark_backend):
    """RTRIM alone."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "hello  "}])
    result = df.select(F_backend.expr("RTRIM(s)").alias("t"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["t"] == "hello"


def test_expr_nested_ltrim_rtrim_with_column(spark, spark_backend):
    """LTRIM(RTRIM(col)) in withColumn."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [{"Name": "Alice", "Value": "  x  "}, {"Name": "Bob", "Value": "  y  "}]
    )
    df = df.withColumn("trimmed", F_backend.expr("LTRIM(RTRIM(Value))"))
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["trimmed"] == "x"
    assert rows[1]["trimmed"] == "y"


def test_expr_rtrim_ltrim_order(spark, spark_backend):
    """RTRIM(LTRIM(col)) - opposite order."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "  hello  "}])
    result = df.select(F_backend.expr("RTRIM(LTRIM(s))").alias("t"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["t"] == "hello"


def test_expr_trim_with_ltrim_rtrim(spark, spark_backend):
    """TRIM works with nested LTRIM/RTRIM - ensure TRIM still works."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "  hello  "}])
    result = df.select(F_backend.expr("TRIM(s)").alias("t"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["t"] == "hello"


def test_expr_ltrim_rtrim_with_filter(spark, spark_backend):
    """LTRIM(RTRIM(col)) then filter."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Value": "  a  "},
            {"Value": "  b  "},
            {"Value": "  c  "},
        ]
    )
    result = df.withColumn("t", F_backend.expr("LTRIM(RTRIM(Value))")).filter(
        F_backend.col("t") == "b"
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["t"] == "b"
