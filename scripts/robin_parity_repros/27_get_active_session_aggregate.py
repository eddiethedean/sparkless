#!/usr/bin/env python3
"""
Repro: getActiveSession or session registry so aggregate functions (sum/avg/count) work.
Sparkless parity tests fail with "No active SparkSession found" when calling F.sum() etc.
after creating a DataFrame with the session.

Run from repo root: python scripts/robin_parity_repros/27_get_active_session_aggregate.py

PySpark equivalent: Create session, createDataFrame, then in same process call F.sum(df.col("x"));
PySpark aggregate functions find the active session.
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
    spark = rs.SparkSession.builder().app_name("repro-active-session").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df([{"x": 1}, {"x": 2}], [("x", "int")])
    try:
        # F.sum(df.col("x")) or df.agg(F.sum("x")) - may require active session in Robin
        out = df.agg(rs.sum(rs.col("x")))
        out.collect()
    except RuntimeError as e:
        if "active" in str(e).lower() and "session" in str(e).lower():
            return False, f"No active SparkSession: {e}"
        raise
    except Exception as e:
        return False, f"agg(sum): {type(e).__name__}: {e}"
    return True, "agg(sum) with session OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(1,), (2,)], ["x"])
        df.agg(F.sum("x")).collect()
        spark.stop()
        return True, "PySpark agg(sum) with session OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
