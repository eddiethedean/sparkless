"""
Smoke test for Sparkless v4 re-export of Robin (robin-sparkless).

Verifies that the standard entry point works:
  from sparkless.sql import SparkSession
  spark = SparkSession.builder().getOrCreate()
  df = spark.createDataFrame([...])
  df.show() / df.collect()
"""

from __future__ import annotations

import pytest


def test_reexport_spark_session_builder_get_or_create() -> None:
    """SparkSession.builder().getOrCreate() returns a session that can create DataFrames."""
    from sparkless.sql import SparkSession

    spark = SparkSession.builder.appName("smoke").getOrCreate()
    assert spark is not None
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["a"] == 1 and rows[0]["b"] == 2
    spark.stop()


def test_reexport_spark_session_one_arg_constructor() -> None:
    """SparkSession(app_name) creates a session (PySpark-style constructor)."""
    from sparkless.sql import SparkSession

    spark = SparkSession("SmokeTest")
    df = spark.createDataFrame([{"x": 10}])
    assert df.collect()[0]["x"] == 10
    spark.stop()


def test_reexport_dataframe_functions() -> None:
    """from sparkless.sql import DataFrame, F works and F.col/lit are usable."""
    from sparkless.sql import SparkSession, DataFrame, F

    spark = SparkSession("F_smoke")
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    assert isinstance(df, DataFrame)
    # Robin uses snake_case; F.col and F.lit should exist
    out = df.select(F.col("a"), F.lit(1).alias("one")).collect()
    assert len(out) == 1
    assert out[0]["a"] == 1 and out[0]["one"] == 1
    spark.stop()
