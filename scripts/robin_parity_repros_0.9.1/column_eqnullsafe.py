#!/usr/bin/env python3
"""
Repro: Column.eqNullSafe() for null-safe equality (PySpark: col.eqNullSafe(lit(None))).
Robin 0.9.1: 'builtins.Column' object has no attribute 'eqNullSafe'.

Run from repo root: python scripts/robin_parity_repros_0.9.1/column_eqnullsafe.py
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
    spark = F.SparkSession.builder().app_name("repro-eqnullsafe").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"a": 1, "b": 2}, {"a": None, "b": None}, {"a": 3, "b": 3}],
        [("a", "int"), ("b", "int")],
    )
    try:
        col_a = F.col("a")
        if hasattr(col_a, "eq_null_safe"):
            out = df.filter(col_a.eq_null_safe(F.lit(None))).collect()
        elif hasattr(col_a, "eqNullSafe"):
            out = df.filter(col_a.eqNullSafe(F.lit(None))).collect()
        else:
            ROBIN_FAIL.append("Column has no eqNullSafe or eq_null_safe")
            return
        ROBIN_OK.append(f"eqNullSafe: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"eqNullSafe: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"a": 1, "b": 2}, {"a": None, "b": None}, {"a": 3, "b": 3}])
        out = df.filter(F.col("a").eqNullSafe(F.lit(None))).collect()
        PYSPARK_OK.append(f"eqNullSafe: {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("column_eqnullsafe: F.col('a').eqNullSafe(F.lit(None))")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
