#!/usr/bin/env python3
"""
Robin issue: between(string_col, num, num) fails (PySpark coerces).

PySpark: col("col").between(1, 20) when col is string coerces for comparison.
Robin: RuntimeError: cannot compare string with numeric type.

Run: python scripts/repro_robin_limitations/17_between_string_numeric.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro-between").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"col": "5"}, {"col": "10"}, {"col": "15"}]
    df = create_df(data, [("col", "str")])
    try:
        df = df.with_column("between", rs.col("col").between(1, 20))
        rows = df.collect()
        return True, f"OK: {rows}"
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
        df = spark.createDataFrame([("5",), ("10",), ("15",)], ["col"])
        df = df.withColumn("between", F.col("col").between(1, 20))
        rows = df.collect()
        return True, f"PySpark: {rows}"
    finally:
        spark.stop()


def main() -> int:
    print("17_between_string_numeric: between(string_col, 1, 20)")
    ok_r, msg_r = run_robin()
    ok_p, msg_p = run_pyspark()
    print("Robin:", "PASS" if ok_r else "FAIL", msg_r)
    print("PySpark:", msg_p)
    return 0 if ok_r else 1


if __name__ == "__main__":
    sys.exit(main())
