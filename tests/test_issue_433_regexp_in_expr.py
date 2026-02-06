"""Test issue #433: REGEXP and RLIKE in F.expr().

PySpark supports df.filter(F.expr("Value REGEXP 'sales|tech'")) and RLIKE.
Sparkless SQLExprParser previously raised ParseException for these expressions.
Now parses REGEXP and RLIKE in F.expr() for PySpark parity.

https://github.com/eddiethedean/sparkless/issues/433
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_filter_regexp_exact_issue_433(spark, spark_backend):
    """Exact scenario from issue #433 - Value REGEXP 'sales|tech'."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales department"},
            {"Name": "Bob", "Value": "tech department"},
            {"Name": "Charlie", "Value": "ceo"},
        ]
    )
    df = df.filter(F_backend.expr("Value REGEXP 'sales|tech'"))
    df.show()
    rows = df.collect()
    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Bob"}


def test_filter_rlike_same_as_regexp(spark, spark_backend):
    """RLIKE is alias for REGEXP in F.expr()."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales"},
            {"Name": "Bob", "Value": "tech"},
            {"Name": "Charlie", "Value": "ceo"},
        ]
    )
    df = df.filter(F_backend.expr("Value RLIKE 'sales|tech'"))
    rows = df.collect()
    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Bob"}


def test_expr_regexp_with_column(spark, spark_backend):
    """F.expr with REGEXP used in withColumn."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales dept"},
            {"Name": "Bob", "Value": "tech dept"},
            {"Name": "Charlie", "Value": "ceo"},
        ]
    )
    df = df.withColumn("matches", F_backend.expr("Value REGEXP 'sales|tech'"))
    rows = df.collect()
    alice = next(r for r in rows if r["Name"] == "Alice")
    bob = next(r for r in rows if r["Name"] == "Bob")
    charlie = next(r for r in rows if r["Name"] == "Charlie")
    assert alice["matches"] is True
    assert bob["matches"] is True
    assert charlie["matches"] is False


def test_expr_regexp_single_match(spark, spark_backend):
    """REGEXP with single pattern match."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "hello world"}, {"s": "goodbye"}])
    df = df.filter(F_backend.expr("s REGEXP 'hello'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "hello world"


def test_expr_regexp_no_match(spark, spark_backend):
    """REGEXP returns empty when no rows match."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
    df = df.filter(F_backend.expr("Name REGEXP 'xyz'"))
    rows = df.collect()
    assert len(rows) == 0


# --- Robust edge-case tests ---


def test_expr_regexp_case_insensitive_keyword(spark, spark_backend):
    """REGEXP and rlike keywords are case-insensitive."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "hello"}, {"s": "world"}])
    df = df.filter(F_backend.expr("s regexp 'hello'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "hello"


def test_expr_regexp_with_nulls(spark, spark_backend):
    """REGEXP with null values - null does not match."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales"},
            {"Name": "Bob", "Value": None},
            {"Name": "Charlie", "Value": "tech"},
        ]
    )
    df = df.filter(F_backend.expr("Value REGEXP 'sales|tech'"))
    rows = df.collect()
    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Charlie"}


def test_expr_regexp_select(spark, spark_backend):
    """select with F.expr REGEXP."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [{"Name": "Alice", "Value": "sales"}, {"Name": "Bob", "Value": "ceo"}]
    )
    result = df.select(
        F_backend.col("Name"),
        F_backend.expr("Value REGEXP 'sales'").alias("is_sales"),
    )
    rows = result.collect()
    assert len(rows) == 2
    alice = next(r for r in rows if r["Name"] == "Alice")
    bob = next(r for r in rows if r["Name"] == "Bob")
    assert alice["is_sales"] is True
    assert bob["is_sales"] is False


def test_expr_regexp_regex_special_chars(spark, spark_backend):
    """REGEXP with regex special characters (character class, quantifier)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "abc123"}, {"s": "xyz"}, {"s": "abc"}])
    df = df.filter(F_backend.expr("s REGEXP '[0-9]+'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "abc123"


def test_expr_regexp_combined_with_and(spark, spark_backend):
    """REGEXP combined with AND."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales dept"},
            {"Name": "Bob", "Value": "tech dept"},
            {"Name": "Charlie", "Value": "sales tech"},
        ]
    )
    df = df.filter(F_backend.expr("Value REGEXP 'sales' AND Value REGEXP 'dept'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["Name"] == "Alice"


def test_expr_regexp_combined_with_or(spark, spark_backend):
    """REGEXP combined with OR."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": "sales"},
            {"Name": "Bob", "Value": "ceo"},
            {"Name": "Charlie", "Value": "tech"},
        ]
    )
    df = df.filter(F_backend.expr("Value REGEXP 'sales' OR Value REGEXP 'tech'"))
    rows = df.collect()
    assert len(rows) == 2
    names = {r["Name"] for r in rows}
    assert names == {"Alice", "Charlie"}


def test_expr_regexp_column_with_underscore(spark, spark_backend):
    """REGEXP with column name containing underscore."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"user_name": "alice"}, {"user_name": "bob"}])
    df = df.filter(F_backend.expr("user_name REGEXP 'alice'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["user_name"] == "alice"


def test_expr_regexp_empty_string_does_not_match(spark, spark_backend):
    """Empty string does not match non-empty pattern."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": ""}, {"s": "hello"}])
    df = df.filter(F_backend.expr("s REGEXP 'hello'"))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "hello"


def test_expr_regexp_filter_after_with_column(spark, spark_backend):
    """Chain filter(REGEXP) after withColumn."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"id": 1, "Value": "sales"},
            {"id": 2, "Value": "tech"},
            {"id": 3, "Value": "ceo"},
        ]
    )
    df = df.withColumn("doubled", F_backend.col("id") * 2).filter(
        F_backend.expr("Value REGEXP 'sales|tech'")
    )
    rows = df.collect()
    assert len(rows) == 2
    ids = {r["id"] for r in rows}
    assert ids == {1, 2}


def test_expr_rlike_with_column_chain(spark, spark_backend):
    """RLIKE in filter then select."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"a": "x1y"}, {"a": "x2y"}, {"a": "z"}])
    result = df.filter(F_backend.expr("a RLIKE 'x[0-9]y'")).select(F_backend.col("a"))
    rows = result.collect()
    assert len(rows) == 2
    values = {r["a"] for r in rows}
    assert values == {"x1y", "x2y"}
