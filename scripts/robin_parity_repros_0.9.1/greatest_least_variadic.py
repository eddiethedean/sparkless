#!/usr/bin/env python3
"""
Repro: greatest() and least() with multiple arguments (variadic).
PySpark: F.greatest(col1, col2, col3), F.least(col1, col2, col3).
Robin 0.9.1: py_greatest() / py_least() take 1 positional argument but 3 were given.

Run from repo root: python scripts/robin_parity_repros_0.9.1/greatest_least_variadic.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ROBIN_OK: list[str] = []
ROBIN_FAIL: list[str] = []
PYSPARK_OK: list[str] = []
PYSPARK_SKIP: list[str] = []


def run_robin() -> None:
    try:
        import robin_sparkless as rs
    except ImportError as e:
        ROBIN_FAIL.append(f"robin_sparkless not installed: {e}")
        return
    F = rs
    spark = F.SparkSession.builder().app_name("repro-greatest-least").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"a": 1, "b": 2, "c": 3}, {"a": 3, "b": 1, "c": 2}, {"a": 2, "b": 3, "c": 1}],
        [("a", "int"), ("b", "int"), ("c", "int")],
    )
    try:
        out = df.with_column("g", F.greatest(F.col("a"), F.col("b"), F.col("c"))).with_column(
            "l", F.least(F.col("a"), F.col("b"), F.col("c"))
        ).collect()
        ROBIN_OK.append(f"greatest/least: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"greatest/least: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame(
            [{"a": 1, "b": 2, "c": 3}, {"a": 3, "b": 1, "c": 2}, {"a": 2, "b": 3, "c": 1}]
        )
        out = df.withColumn("g", F.greatest(F.col("a"), F.col("b"), F.col("c"))).withColumn(
            "l", F.least(F.col("a"), F.col("b"), F.col("c"))
        ).collect()
        PYSPARK_OK.append(f"greatest/least: {len(out)} rows, e.g. {out[0]}")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark error: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("greatest_least_variadic: F.greatest(a,b,c) and F.least(a,b,c)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
