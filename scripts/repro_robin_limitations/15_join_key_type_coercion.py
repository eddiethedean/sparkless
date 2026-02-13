#!/usr/bin/env python3
"""
Robin issue: Join requires exact key type match (PySpark coerces).

PySpark: join on str vs i64 coerces.
Robin: RuntimeError: datatypes of join keys don't match.

Run: python scripts/repro_robin_limitations/15_join_key_type_coercion.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("repro-join").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df1 = create_df([{"id": "1", "label": "a"}], [("id", "str"), ("label", "str")])
    df2 = create_df([{"id": 1, "x": 10}], [("id", "int"), ("x", "int")])
    try:
        joined = df1.join(df2, on=["id"], how="inner")
        rows = joined.collect()
        return True, f"OK: {rows}"
    except Exception as e:
        return False, f"Robin ERROR: {type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return False, "pyspark not installed"
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df1 = spark.createDataFrame([("1", "a")], ["id", "label"])
        df2 = spark.createDataFrame([(1, 10)], ["id", "x"])
        joined = df1.join(df2, on="id", how="inner")
        rows = joined.collect()
        return True, f"PySpark: {rows}"
    finally:
        spark.stop()


def main() -> int:
    print("15_join_key_type_coercion: join str vs i64 keys")
    ok_r, msg_r = run_robin()
    ok_p, msg_p = run_pyspark()
    print("Robin:", "PASS" if ok_r else "FAIL", msg_r)
    print("PySpark:", msg_p)
    return 0 if ok_r else 1


if __name__ == "__main__":
    sys.exit(main())
