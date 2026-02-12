#!/usr/bin/env python3
"""
Repro: create_dataframe_from_rows with array/list column in schema.
PySpark accepts list data and infers or accepts ArrayType; Robin rejects schema dtype 'list' or 'array'.

Run from repo root: python scripts/robin_parity_repros/08_create_dataframe_array_column.py
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
    spark = F.SparkSession.builder().app_name("repro-array-col").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"name": "a", "vals": [1, 2, 3]}]
    # Robin rejects schema with 'list' or 'array' dtype for the column
    for dtype in ("list", "array"):
        try:
            df = create_df(data, [("name", "string"), ("vals", dtype)])
            out = df.collect()
            ROBIN_OK.append(f"create_df with schema vals={dtype}: {len(out)} rows")
        except Exception as e:
            ROBIN_FAIL.append(f"create_df with schema vals={dtype}: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"name": "a", "vals": [1, 2, 3]}])
        out = df.collect()
        PYSPARK_OK.append(f"createDataFrame with list column: {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("08_create_dataframe_array_column: create_dataframe_from_rows with array/list schema")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    if ROBIN_FAIL and PYSPARK_OK:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
