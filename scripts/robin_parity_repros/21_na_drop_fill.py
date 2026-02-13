#!/usr/bin/env python3
"""
Repro: DataFrame.na.drop() and na.fill() with subset, how, thresh (PySpark parity).
Sparkless tests skip when df.na.drop(subset=...) or na.fill() not available.

Run from repo root: python scripts/robin_parity_repros/21_na_drop_fill.py

PySpark equivalent:
  df.na.drop(subset=["x"])
  df.na.fill(0, subset=["y"])
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
    spark = rs.SparkSession.builder().app_name("repro-na").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"x": 1, "y": None}, {"x": None, "y": 2}, {"x": 3, "y": 3}],
        [("x", "int"), ("y", "int")],
    )
    if not hasattr(df, "na"):
        return False, "DataFrame has no attribute 'na'"
    na = df.na
    if callable(na):
        na = na()
    if not hasattr(na, "drop"):
        return False, "df.na has no attribute 'drop'"
    try:
        out = na.drop(subset=["x"])
        out.collect()
    except TypeError as e:
        if "subset" in str(e) or "unexpected keyword" in str(e).lower():
            return False, f"na.drop(subset=...): {e}"
        raise
    except Exception as e:
        return False, f"na.drop: {type(e).__name__}: {e}"
    if not hasattr(na, "fill"):
        return False, "df.na has no attribute 'fill'"
    try:
        out = na.fill(0, subset=["y"])
        out.collect()
    except TypeError as e:
        if "subset" in str(e) or "unexpected keyword" in str(e).lower():
            return False, f"na.fill(subset=...): {e}"
        raise
    except Exception as e:
        return False, f"na.fill: {type(e).__name__}: {e}"
    return True, "na.drop and na.fill with subset OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(1, None), (None, 2), (3, 3)], ["x", "y"])
        df.na.drop(subset=["x"]).collect()
        df.na.fill(0, subset=["y"]).collect()
        spark.stop()
        return True, "PySpark na.drop/na.fill(subset=...) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
