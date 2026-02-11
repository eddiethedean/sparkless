#!/usr/bin/env python3
"""
Limitation: datetime in row — create_dataframe_from_rows with datetime value
may raise (JSON serialization or "row values must be scalar").

Run from repo root: python scripts/repro_robin_limitations/10_datetime_row.py
"""

from __future__ import annotations

import sys
from datetime import datetime
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
    spark = rs.SparkSession.builder().app_name("repro-datetime").get_or_create()
    dt = datetime(2025, 2, 10, 12, 0, 0)
    data = [{"id": 1, "ts": dt}, {"id": 2, "ts": None}]
    schema = [("id", "int"), ("ts", "timestamp")]
    try:
        create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
        df = create_df(data, schema)
        out = df.collect()
        ROBIN_OK.append(f"create_dataframe_from_rows with datetime: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"create_dataframe_from_rows with datetime: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        from datetime import datetime as dt
        df = spark.createDataFrame([(1, dt(2025, 2, 10, 12, 0, 0)), (2, None)], ["id", "ts"])
        out = df.collect()
        PYSPARK_OK.append(f"createDataFrame with datetime: {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("10_datetime_row: create_dataframe_from_rows with datetime column")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
