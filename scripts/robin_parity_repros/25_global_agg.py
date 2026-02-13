#!/usr/bin/env python3
"""
Repro: DataFrame.agg(*exprs) for global aggregation (no groupBy) - PySpark parity.
Sparkless parity tests fail with "'DataFrame' object has no attribute 'agg'".

Run from repo root: python scripts/robin_parity_repros/25_global_agg.py

PySpark equivalent:
  df.agg(F.sum("x"), F.avg("y"))
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
    spark = rs.SparkSession.builder().app_name("repro-agg").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df([{"x": 1, "y": 10}, {"x": 2, "y": 20}], [("x", "int"), ("y", "int")])
    if not hasattr(df, "agg"):
        return False, "DataFrame has no attribute 'agg'"
    try:
        out = df.agg(rs.sum(rs.col("x")), rs.avg(rs.col("y")))
        out.collect()
    except Exception as e:
        return False, f"df.agg(): {type(e).__name__}: {e}"
    return True, "DataFrame.agg() OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(1, 10), (2, 20)], ["x", "y"])
        df.agg(F.sum("x"), F.avg("y")).collect()
        spark.stop()
        return True, "PySpark df.agg() OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
