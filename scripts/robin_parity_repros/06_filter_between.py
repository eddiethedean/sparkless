#!/usr/bin/env python3
"""
Repro: filter with between(). Sparkless fails with "Operation 'Operations: filter'
is not supported" for between(); verify Robin supports col.between(low, high).

Run from repo root: python scripts/robin_parity_repros/06_filter_between.py
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
    spark = F.SparkSession.builder().app_name("repro-between").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"v": 10}, {"v": 25}, {"v": 50}],
        [("v", "int")],
    )
    try:
        if hasattr(F.col("v"), "between"):
            out = df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()
        else:
            ROBIN_FAIL.append("col.between not found")
            return
        ROBIN_OK.append(f"filter(between(20, 30)): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"filter(between): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"v": 10}, {"v": 25}, {"v": 50}])
        out = df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()
        PYSPARK_OK.append(f"filter(between): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("06_filter_between: filter with between(low, high)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
