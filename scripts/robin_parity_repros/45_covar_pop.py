#!/usr/bin/env python3
"""Repro: covar_pop(expr1, expr2) — PySpark aggregate. Run from repo root."""
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
    spark = rs.SparkSession.builder().app_name("repro-covar_pop").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df([{"k": "a", "x": 1, "y": 1}, {"k": "a", "x": 2, "y": 2}, {"k": "a", "x": 3, "y": 3}], [("k", "string"), ("x", "int"), ("y", "int")])
    fn = getattr(rs, "covar_pop", None)
    if not fn:
        return False, "robin_sparkless has no covar_pop"
    try:
        df.groupBy("k").agg(fn(rs.col("x"), rs.col("y")).alias("c")).collect()
        return True, "covar_pop OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", 1, 1), ("a", 2, 2), ("a", 3, 3)], ["k", "x", "y"])
        df.groupBy("k").agg(F.covar_pop("x", "y").alias("c")).collect()
        spark.stop()
        return True, "PySpark covar_pop OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"

if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
