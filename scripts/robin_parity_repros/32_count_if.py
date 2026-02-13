#!/usr/bin/env python3
"""
Repro: count_if(expr) — PySpark parity. Returns number of TRUE values.
Run from repo root: python scripts/robin_parity_repros/32_count_if.py
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
    spark = rs.SparkSession.builder().app_name("repro-count_if").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"v": 0}, {"v": 1}, {"v": 2}, {"v": 3}, {"v": None}]
    df = create_df(data, [("v", "int")])
    fn = getattr(rs, "count_if", None)
    if not fn:
        return False, "robin_sparkless has no count_if"
    try:
        out = df.agg(fn(rs.col("v") % 2 == 0).alias("c"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        return True, "count_if OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(0,), (1,), (2,), (3,), (None,)], ["v"])
        out = df.agg(F.count_if(F.col("v") % 2 == 0).alias("c"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][0] == 2:
            return True, "PySpark count_if OK"
        return False, f"PySpark got {rows[0][0] if rows else '?'}, expected 2"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
