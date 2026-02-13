#!/usr/bin/env python3
"""
Robin issue: round() on string column returns None (PySpark strips and casts).

PySpark: F.round("  10.6  ") strips whitespace, casts to numeric, returns 11.0.
Robin: Returns None for string columns.

Run: python scripts/repro_robin_limitations/13_round_string_returns_none.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro-round").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"val": "  10.6  "}, {"val": "\t20.7"}]
    df = create_df(data, [("val", "str")])
    df = df.with_column("rounded", rs.round(rs.col("val")))
    rows = df.collect()
    rounded = [r["rounded"] for r in rows]
    if rounded == [11.0, 21.0]:
        return True, f"OK: {rounded}"
    return False, f"Robin: {rounded} (expected [11.0, 21.0])"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        return False, "pyspark not installed"
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([("  10.6  ",), ("\t20.7\n",)], ["val"])
        df = df.withColumn("rounded", F.round("val"))
        rows = df.collect()
        rounded = [r["rounded"] for r in rows]
        return True, f"PySpark: {rounded}"
    finally:
        spark.stop()


def main() -> int:
    print("13_round_string_returns_none: round() on string column")
    ok_r, msg_r = run_robin()
    ok_p, msg_p = run_pyspark()
    print("Robin:", "PASS" if ok_r else "FAIL", msg_r)
    print("PySpark:", msg_p)
    return 0 if ok_r else 1


if __name__ == "__main__":
    sys.exit(main())
