#!/usr/bin/env python3
"""
Robin issue: create_map() requires at least one argument (PySpark supports empty).

PySpark: F.create_map() returns empty map column.
Robin: TypeError: py_create_map() missing 1 required positional argument.

Run: python scripts/repro_robin_limitations/16_create_map_empty.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro-map").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"id": 1}]
    df = create_df(data, [("id", "int")])
    try:
        df = df.with_column("m", rs.create_map())
        rows = df.collect()
        m = rows[0].get("m")
        if m == {} or m == []:
            return True, f"OK: m={m}"
        return False, f"Robin: m={m} (expected {{}})"
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
        df = spark.createDataFrame([(1,)], ["id"])
        df = df.withColumn("m", F.create_map())
        rows = df.collect()
        return True, f"PySpark: m={rows[0]['m']}"
    finally:
        spark.stop()


def main() -> int:
    print("16_create_map_empty: create_map() with no args")
    ok_r, msg_r = run_robin()
    ok_p, msg_p = run_pyspark()
    print("Robin:", "PASS" if ok_r else "FAIL", msg_r)
    print("PySpark:", msg_p)
    return 0 if ok_r else 1


if __name__ == "__main__":
    sys.exit(main())
