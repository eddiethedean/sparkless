#!/usr/bin/env python3
"""Repro: bool_and(expr) / every(expr) — PySpark aggregate. Run from repo root."""
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
    spark = rs.SparkSession.builder().app_name("repro-bool_and").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df([{"k": "a", "v": True}, {"k": "a", "v": True}], [("k", "string"), ("v", "boolean")])
    fn = getattr(rs, "bool_and", None) or getattr(rs, "every", None)
    if not fn:
        return False, "robin_sparkless has no bool_and or every"
    try:
        df.groupBy("k").agg(fn(rs.col("v")).alias("ba")).collect()
        return True, "bool_and/every OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", True), ("a", True)], ["k", "v"])
        df.groupBy("k").agg(F.every("v").alias("ba")).collect()
        spark.stop()
        return True, "PySpark every OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"

if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
