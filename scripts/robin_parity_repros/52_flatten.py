#!/usr/bin/env python3
"""Repro: flatten(col) — PySpark array scalar (array of arrays). Run from repo root."""

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
    spark = rs.SparkSession.builder().app_name("repro-flatten").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    data = [{"nested": [[1, 2], [3, 4]]}]
    try:
        df = create_df(data, [("nested", "array")])
    except Exception as e:
        return False, f"create_df: {e}"
    fn = getattr(rs, "flatten", None)
    if not fn:
        return False, "robin_sparkless has no flatten"
    try:
        out = df.select(fn(rs.col("nested")).alias("flat"))
        out.collect()
        return True, "flatten OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([([[1, 2], [3, 4]],)], "nested array<array<int>>")
        out = df.select(F.flatten("nested").alias("flat"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][0] == [1, 2, 3, 4]:
            return True, "PySpark flatten OK"
        return False, f"PySpark got {rows[0][0] if rows else '?'}"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"

if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
