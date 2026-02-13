#!/usr/bin/env python3
"""
Robin issue: to_timestamp() on TimestampType column fails (expects String).

PySpark: to_timestamp(timestamp_col) passes through unchanged.
Robin: TypeError or RuntimeError expecting string.

Run: python scripts/repro_robin_limitations/18_to_timestamp_timestamp_col.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    import robin_sparkless as rs
    from datetime import datetime

    spark = rs.SparkSession.builder().app_name("repro-ts2").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"ts": datetime(2024, 1, 1, 10, 0, 0)}]
    df = create_df(data, [("ts", "datetime")])
    try:
        df = df.with_column("ts2", rs.to_timestamp(rs.col("ts")))
        rows = df.collect()
        return True, f"OK: {rows}"
    except Exception as e:
        return False, f"Robin ERROR: {type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from datetime import datetime
    except ImportError:
        return False, "pyspark not installed"
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([(datetime(2024, 1, 1, 10, 0, 0),)], ["ts"])
        df = df.withColumn("ts2", F.to_timestamp(F.col("ts")))
        rows = df.collect()
        return True, f"PySpark: {rows}"
    finally:
        spark.stop()


def main() -> int:
    print("18_to_timestamp_timestamp_col: to_timestamp(timestamp_col)")
    ok_r, msg_r = run_robin()
    ok_p, msg_p = run_pyspark()
    print("Robin:", "PASS" if ok_r else "FAIL", msg_r)
    print("PySpark:", msg_p)
    return 0 if ok_r else 1


if __name__ == "__main__":
    sys.exit(main())
