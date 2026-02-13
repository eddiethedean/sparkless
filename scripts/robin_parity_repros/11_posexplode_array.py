#!/usr/bin/env python3
"""
Repro: posexplode on array column. PySpark explodes array into pos + col columns;
Robin may raise "invalid series dtype: expected List, got str" if array not supported.

Run from repo root: python scripts/robin_parity_repros/11_posexplode_array.py
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
    spark = F.SparkSession.builder().app_name("repro-posexplode").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    # Robin may reject list/array schema; try with struct or fallback
    data = [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}]
    try:
        df = create_df(data, [("Name", "string"), ("Values", "array")])
    except Exception as e:
        ROBIN_FAIL.append(f"create_df with array: {type(e).__name__}: {e}")
        return
    try:
        out = df.select("Name", F.posexplode("Values").alias("pos", "val")).collect()
        ROBIN_OK.append(f"posexplode: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"posexplode: {type(e).__name__}: {e}")


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
            [{"Name": "Alice", "Values": [10, 20]}, {"Name": "Bob", "Values": [30, 40]}]
        )
        out = df.select("Name", F.posexplode("Values").alias("pos", "val")).collect()
        PYSPARK_OK.append(f"posexplode: {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("11_posexplode_array: posexplode on array column")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    if ROBIN_FAIL and PYSPARK_OK:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
