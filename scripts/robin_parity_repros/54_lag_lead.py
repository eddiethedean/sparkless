#!/usr/bin/env python3
"""Repro: lag(col, offset) — PySpark window. Run from repo root."""
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
    spark = rs.SparkSession.builder().app_name("repro-lag").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df([{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "a", "v": 3}], [("k", "string"), ("v", "int")])
    lag_fn = getattr(rs, "lag", None)
    w = getattr(rs, "Window", None)
    if not lag_fn or not w:
        return False, "robin_sparkless has no lag or Window"
    try:
        win = w.partition_by(rs.col("k")).order_by(rs.col("v"))
        df.select("k", "v", lag_fn(rs.col("v"), 1).over(win).alias("prev")).collect()
        return True, "lag OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", 1), ("a", 2), ("a", 3)], ["k", "v"])
        win = Window.partitionBy("k").orderBy("v")
        rows = df.select("k", "v", F.lag("v", 1).over(win).alias("prev")).collect()
        spark.stop()
        return (True, "PySpark lag OK") if len(rows) > 1 and rows[1][2] == 1 else (False, f"got {rows[1][2] if len(rows) > 1 else '?'}")
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"

if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
