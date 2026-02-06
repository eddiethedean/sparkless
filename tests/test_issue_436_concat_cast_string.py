"""Test issue #436: concat with literal + cast(StringType).

F.concat(F.lit('x'), F.round(col, 2).cast(StringType()), F.lit('y')) raised
TypeError: cannot create expression literal for value of type StringType.
concat was treating ColumnOperation (cast) as Literal and calling pl.lit(col.value)
where col.value was StringType. Must translate Column/ColumnOperation, not pl.lit.

https://github.com/eddiethedean/sparkless/issues/436
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_concat_literal_cast_string_exact_issue_436(spark, spark_backend):
    """Exact scenario from issue #436 - concat with lit + cast(StringType) + lit."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.StringType
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": 123.456},
            {"Name": "Bob", "Value": 789.012},
        ]
    )
    df = df.select(
        F.col("Name"),
        F.concat(
            F.lit("Value Is: "),
            F.round(F.col("Value"), 2).cast(T()),
            F.lit(" Dollars"),
        ).alias("TextColumn"),
    )
    df.show(truncate=False)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["TextColumn"] == "Value Is: 123.46 Dollars"
    assert rows[1]["TextColumn"] == "Value Is: 789.01 Dollars"


def test_concat_literal_col_cast_string(spark, spark_backend):
    """concat with lit + col.cast(StringType) + lit."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.StringType
    df = spark.createDataFrame([{"n": 42}])
    result = df.select(
        F.concat(F.lit("num: "), F.col("n").cast(T()), F.lit("!")).alias("s")
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "num: 42!"


def test_concat_all_literals_still_works(spark, spark_backend):
    """concat with all literals - regression check."""
    F = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"x": 1}])
    result = df.select(F.concat(F.lit("a"), F.lit("b"), F.lit("c")).alias("s"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "abc"


def test_concat_all_columns_still_works(spark, spark_backend):
    """concat with all column refs - regression check."""
    F = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"a": "x", "b": "y"}])
    result = df.select(F.concat(F.col("a"), F.col("b")).alias("s"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "xy"


def test_concat_expression_cast_string(spark, spark_backend):
    """concat with expression (e.g. upper) cast to string."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.StringType
    df = spark.createDataFrame([{"name": "alice"}])
    result = df.select(
        F.concat(
            F.lit("Hello "),
            F.upper(F.col("name")).cast(T()),
            F.lit("!"),
        ).alias("greeting")
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["greeting"] == "Hello ALICE!"
