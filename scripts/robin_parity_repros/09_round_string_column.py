#!/usr/bin/env python3
"""
Repro: F.round() on string column. PySpark implicitly casts string to numeric;
Robin may raise "round can only be used on numeric types".

Run from repo root: python scripts/robin_parity_repros/09_round_string_column.py
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
    spark = F.SparkSession.builder().app_name("repro-round-string").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"val": "10.4"}, {"val": "9.6"}],
        [("val", "string")],
    )
    try:
        out = df.with_column("rounded", F.round(F.col("val"))).collect()
        ROBIN_OK.append(f"round(string): {[r['rounded'] for r in out]}")
    except Exception as e:
        ROBIN_FAIL.append(f"round(string): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"val": "10.4"}, {"val": "9.6"}])
        out = df.withColumn("rounded", F.round("val")).collect()
        PYSPARK_OK.append(f"round(string): {[r['rounded'] for r in out]} (PySpark implicit cast)")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("09_round_string_column: F.round() on string column")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    if ROBIN_FAIL and PYSPARK_OK:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
