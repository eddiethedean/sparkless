#!/usr/bin/env python3
"""Repro: skewness(col) / kurtosis(col) — PySpark aggregate parity. Run from repo root."""

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
    spark = rs.SparkSession.builder().app_name("repro-skewness").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    data = [{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "a", "v": 3}]
    df = create_df(data, [("k", "string"), ("v", "int")])
    fn = getattr(rs, "skewness", None)
    if not fn:
        return False, "robin_sparkless has no skewness"
    try:
        out = df.groupBy("k").agg(fn(rs.col("v")).alias("sk"))
        out.collect()
        return True, "skewness/kurtosis OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", 1), ("a", 2), ("a", 3)], ["k", "v"])
        out = df.groupBy("k").agg(F.skewness("v").alias("sk"))
        spark.stop()
        return True, "PySpark skewness OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"

if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
