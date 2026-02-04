"""Tests for Issue #408: MockRDD.flatMap support."""

from __future__ import annotations


def test_rdd_flatmap_words(spark) -> None:
    """Reproduce issue #408: df.rdd.flatMap splits lines into words."""
    df = spark.createDataFrame(
        [("hello world",), ("hello pyspark",), ("flatMap is useful",)],
        ["line"],
    )
    rdd = df.rdd.flatMap(lambda row: row["line"].split())
    assert rdd.collect() == [
        "hello",
        "world",
        "hello",
        "pyspark",
        "flatMap",
        "is",
        "useful",
    ]


def test_rdd_flatmap_empty_iterable(spark) -> None:
    """flatMap with func that sometimes returns empty iterable."""
    df = spark.createDataFrame([(1,), (0,), (2,)], ["x"])
    rdd = df.rdd.flatMap(lambda row: range(row["x"]) if row["x"] > 0 else [])
    assert rdd.collect() == [0, 0, 1]


def test_rdd_flatmap_then_map(spark) -> None:
    """flatMap then map (chaining)."""
    df = spark.createDataFrame([("a b",), ("c",)], ["line"])
    rdd = df.rdd.flatMap(lambda row: row["line"].split()).map(lambda s: s.upper())
    assert rdd.collect() == ["A", "B", "C"]
