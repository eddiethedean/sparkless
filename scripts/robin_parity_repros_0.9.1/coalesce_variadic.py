#!/usr/bin/env python3
"""
Repro: coalesce() with multiple arguments (variadic).
PySpark: F.coalesce(col1, col2, lit(0)).
Robin 0.9.1: py_coalesce() takes 1 positional argument but 2 were given.

Run from repo root: python scripts/robin_parity_repros_0.9.1/coalesce_variadic.py
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
    spark = F.SparkSession.builder().app_name("repro-coalesce").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"a": 1, "b": None}, {"a": None, "b": 2}, {"a": None, "b": None}],
        [("a", "int"), ("b", "int")],
    )
    try:
        out = df.with_column("c", F.coalesce(F.col("a"), F.col("b"))).collect()
        ROBIN_OK.append(f"coalesce: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"coalesce: {type(e).__name__}: {e}")


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
            [{"a": 1, "b": None}, {"a": None, "b": 2}, {"a": None, "b": None}]
        )
        out = df.withColumn("c", F.coalesce(F.col("a"), F.col("b"))).collect()
        PYSPARK_OK.append(f"coalesce: {len(out)} rows, e.g. {out[0]}")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark error: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("coalesce_variadic: F.coalesce(col1, col2)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
