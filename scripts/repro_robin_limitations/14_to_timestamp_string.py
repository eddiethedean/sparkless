#!/usr/bin/env python3
"""
Robin issue: to_timestamp() on string column fails or returns None.

PySpark: F.to_timestamp(col("ts_str")) parses "2024-01-01 10:00:00".
Robin: RuntimeError or returns None.

Run: python scripts/repro_robin_limitations/14_to_timestamp_string.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro-ts").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"ts_str": "2024-01-01 10:00:00"}]
    df = create_df(data, [("ts_str", "str")])
    try:
        df = df.with_column("ts", rs.to_timestamp(rs.col("ts_str")))
        rows = df.collect()
        ts = rows[0].get("ts")
        if ts is not None and "2024" in str(ts):
            return True, f"OK: {ts}"
        return False, f"Robin: ts={ts} (expected datetime)"
    except Exception as e:
        return False, f"Robin ERROR: {type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        return False, "pyspark not installed"
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([("2024-01-01 10:00:00",)], ["ts_str"])
        df = df.withColumn("ts", F.to_timestamp(F.col("ts_str")))
        rows = df.collect()
        return True, f"PySpark: ts={rows[0]['ts']}"
    finally:
        spark.stop()


def main() -> int:
    print("14_to_timestamp_string: to_timestamp(string)")
    ok_r, msg_r = run_robin()
    ok_p, msg_p = run_pyspark()
    print("Robin:", "PASS" if ok_r else "FAIL", msg_r)
    print("PySpark:", msg_p)
    return 0 if ok_r else 1


if __name__ == "__main__":
    sys.exit(main())
