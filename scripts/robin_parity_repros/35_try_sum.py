#!/usr/bin/env python3
"""
Repro: try_sum(expr) — PySpark parity. Sum, null on overflow.
Run from repo root: python scripts/robin_parity_repros/35_try_sum.py
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
    spark = rs.SparkSession.builder().app_name("repro-try_sum").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"v": 1}, {"v": 2}, {"v": 3}]
    df = create_df(data, [("v", "int")])
    fn = getattr(rs, "try_sum", None)
    if not fn:
        return False, "robin_sparkless has no try_sum"
    try:
        out = df.agg(fn(rs.col("v")).alias("s"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        return True, "try_sum OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(1,), (2,), (3,)], ["v"])
        out = df.agg(F.try_sum("v").alias("s"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][0] == 6:
            return True, "PySpark try_sum OK"
        return False, f"PySpark got {rows[0][0] if rows else '?'}, expected 6"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
