#!/usr/bin/env python3
"""
Repro: string_agg(expr [, delimiter]) — PySpark 4.0+ aggregate.
Run from repo root: python scripts/robin_parity_repros/34_string_agg.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> tuple[bool, str]:
    try:
        import robin_sparkless as rs
    except ImportError as e:
        return False, f"robin_sparkless not installed: {e}"
    spark = rs.SparkSession.builder().app_name("repro-string_agg").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"k": "a", "s": "x"}, {"k": "a", "s": "y"}, {"k": "a", "s": "z"}]
    df = create_df(data, [("k", "string"), ("s", "string")])
    fn = getattr(rs, "string_agg", None)
    if not fn:
        return False, "robin_sparkless has no string_agg"
    try:
        out = df.groupBy("k").agg(fn(rs.col("s"), ",").alias("agg"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        return True, "string_agg OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", "x"), ("a", "y"), ("a", "z")], ["k", "s"])
        out = df.groupBy("k").agg(F.string_agg("s", ",").alias("agg"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][1] == "x,y,z":
            return True, "PySpark string_agg OK"
        return False, f"PySpark got {rows[0][1] if rows else '?'}"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
