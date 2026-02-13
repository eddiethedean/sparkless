#!/usr/bin/env python3
"""Repro: datediff(end, start) — PySpark scalar. Run from repo root."""
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
    spark = rs.SparkSession.builder().app_name("repro-datediff").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df([{"end": "2024-02-10", "start": "2024-02-01"}], [("end", "string"), ("start", "string")])
    fn = getattr(rs, "datediff", None)
    if not fn:
        return False, "robin_sparkless has no datediff"
    try:
        out = df.select(fn(rs.col("end"), rs.col("start")).alias("d"))
        return True, "datediff OK" if out.collect() else "no rows"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("2024-02-10", "2024-02-01")], ["end", "start"])
        out = df.select(F.datediff(F.to_date("end"), F.to_date("start")).alias("d"))
        spark.stop()
        return True, "PySpark datediff OK" if out.collect()[0][0] == 9 else "wrong value"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"

if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
