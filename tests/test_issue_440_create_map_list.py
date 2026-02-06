"""Test issue #440: F.create_map() accepts list with alternating key-value pairs.

PySpark's create_map accepts both:
  - create_map(k1, v1, k2, v2, ...)  # variadic
  - create_map([k1, v1, k2, v2, ...])  # single list

Run in PySpark mode first, then mock mode:
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/test_issue_440_create_map_list.py -v
  pytest tests/test_issue_440_create_map_list.py -v

https://github.com/eddiethedean/sparkless/issues/440
"""

from itertools import chain

from tests.fixtures.spark_imports import get_spark_imports


def test_create_map_list_exact_issue_440(spark, spark_backend):
    """Exact scenario from issue #440 - create_map with list from chain(*dict.items())."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": 1},
            {"Name": "Bob", "Value": 3},
        ]
    )

    translation_dict = {1: "Small", 2: "Medium", 3: "Large"}
    mapping = F.create_map([F.lit(x) for x in chain(*translation_dict.items())])

    df = df.withColumn("Size", mapping[F.col("Value")])
    rows = df.collect()

    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice"
    assert rows[0]["Value"] == 1
    assert rows[0]["Size"] == "Small"
    assert rows[1]["Name"] == "Bob"
    assert rows[1]["Value"] == 3
    assert rows[1]["Size"] == "Large"


def test_create_map_list_literals_only(spark, spark_backend):
    """create_map([lit(k1), lit(v1), lit(k2), lit(v2)]) - all literals."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"x": 1}])
    m = F.create_map([F.lit("a"), F.lit(10), F.lit("b"), F.lit(20)])
    result = df.select(m.alias("map_col"))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["map_col"] == {"a": 10, "b": 20}


def test_create_map_list_mixed_literals_columns(spark, spark_backend):
    """create_map([lit(k1), col(v1), lit(k2), col(v2)]) - mixed."""
    F = get_spark_imports(spark_backend).F

    df = spark.createDataFrame([{"v1": "x", "v2": "y"}])
    m = F.create_map([F.lit("k1"), F.col("v1"), F.lit("k2"), F.col("v2")])
    result = df.select(m.alias("map_col"))
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["map_col"] == {"k1": "x", "k2": "y"}
