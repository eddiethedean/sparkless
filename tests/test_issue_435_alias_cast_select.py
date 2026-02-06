"""Test issue #435: alias().cast() in select.

F.col("ValueOld").alias("ValueNew").cast(IntegerType()) raised SparkColumnNotFoundError:
cannot resolve 'ValueNew' given input columns: [Name, ValueOld].
PySpark supports this combination - alias is output name, validation should use original column.

https://github.com/eddiethedean/sparkless/issues/435
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_alias_cast_select_exact_issue_435(spark, spark_backend):
    """Exact scenario from issue #435 - alias().cast() in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "ValueOld": "123"},
            {"Name": "Bob", "ValueOld": "456"},
        ]
    )
    df = df.select(
        F.col("Name"),
        F.col("ValueOld").alias("ValueNew").cast(T()),
    )
    df.show()
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice" and rows[0]["ValueNew"] == 123
    assert rows[1]["Name"] == "Bob" and rows[1]["ValueNew"] == 456


def test_alias_without_cast_still_works(spark, spark_backend):
    """alias() without cast - regression check."""
    F = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    result = df.select(
        F.col("a"),
        F.col("b").alias("B_renamed"),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["B_renamed"] == 2


def test_cast_without_alias_still_works(spark, spark_backend):
    """cast() without alias - regression check."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"s": "123"}])
    result = df.select(F.col("s").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == 123


def test_alias_cast_string_type(spark, spark_backend):
    """alias().cast(StringType()) in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.StringType
    df = spark.createDataFrame([{"num": 123}])
    result = df.select(F.col("num").alias("num_str").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "123"


def test_alias_cast_multiple_columns(spark, spark_backend):
    """Multiple alias().cast() in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"a": "1", "b": "2", "c": "3"}])
    result = df.select(
        F.col("a").alias("A_int").cast(T()),
        F.col("b").alias("B_int").cast(T()),
        F.col("c").alias("C_int").cast(T()),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["A_int"] == 1 and rows[0]["B_int"] == 2 and rows[0]["C_int"] == 3
