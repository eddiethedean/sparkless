#!/usr/bin/env python3
"""
Repro: approx_percentile(col, percentage [, accuracy]) — PySpark parity.
Spark built-in aggregate. Run from repo root: python scripts/robin_parity_repros/30_approx_percentile.py
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
    create_df = None
    try:
        spark = rs.SparkSession.builder().app_name("repro-approx_percentile").get_or_create()
        create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
            spark, "_create_dataframe_from_rows"
        )
    except Exception as e:
        return False, f"session: {e}"
    data = [{"v": i} for i in (0, 1, 2, 10)]
    df = create_df(data, [("v", "int")])
    fn = getattr(rs, "approx_percentile", None)
    if not fn:
        return False, "robin_sparkless has no approx_percentile"
    try:
        out = df.agg(fn(rs.col("v"), 0.5).alias("p"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        return True, "approx_percentile(0.5) OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(i,) for i in (0, 1, 2, 10)], ["v"])
        out = df.agg(F.approx_percentile("v", 0.5).alias("p"))
        rows = out.collect()
        spark.stop()
        return True, "PySpark approx_percentile(0.5) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
