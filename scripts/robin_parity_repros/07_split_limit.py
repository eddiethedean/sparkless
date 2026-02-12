#!/usr/bin/env python3
"""
Repro: F.split(column, pattern, limit). PySpark supports split with optional limit;
Robin may not (RuntimeError: not found: split(..., limit)).

Run from repo root: python scripts/robin_parity_repros/07_split_limit.py
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
    spark = F.SparkSession.builder().app_name("repro-split").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"s": "a,b,c"}],
        [("s", "string")],
    )
    try:
        out = df.select(F.split(F.col("s"), ",", 2)).collect()
        ROBIN_OK.append(f"split(s, ',', 2): {out}")
    except Exception as e:
        ROBIN_FAIL.append(f"split(s, ',', 2): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"s": "a,b,c"}])
        out = df.select(F.split(F.col("s"), ",", 2)).collect()
        PYSPARK_OK.append(f"split(s, ',', 2): {out}")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("07_split_limit: F.split(column, pattern, limit)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    # Exit 1 when Robin failed and PySpark passed (verified gap)
    if ROBIN_FAIL and PYSPARK_OK:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
