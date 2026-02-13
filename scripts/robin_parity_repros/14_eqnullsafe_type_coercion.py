#!/usr/bin/env python3
"""
Repro: eqNullSafe with type coercion (string column vs numeric literal).
PySpark coerces; Robin may raise "cannot compare string with numeric type (i64)".

Run from repo root: python scripts/robin_parity_repros/14_eqnullsafe_type_coercion.py
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
        [{"str_col": "123", "other": 1}, {"str_col": "456", "other": 2}],
        [("str_col", "string"), ("other", "int")],
    )
    try:
        out = df.select(F.col("str_col").eq_null_safe(F.lit(123)).alias("eq")).collect()
        ROBIN_OK.append(f"eqNullSafe(str, int): {[r['eq'] for r in out]}")
    except Exception as e:
        ROBIN_FAIL.append(f"eqNullSafe(str, int): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"str_col": "123"}, {"str_col": "456"}])
        out = df.select(F.col("str_col").eqNullSafe(F.lit(123)).alias("eq")).collect()
        PYSPARK_OK.append(f"eqNullSafe(str, int): {[r['eq'] for r in out]} (PySpark coerces)")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("14_eqnullsafe_type_coercion: eqNullSafe(string_col, int_literal)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    if ROBIN_FAIL and PYSPARK_OK:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
