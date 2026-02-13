#!/usr/bin/env python3
"""
Repro: first() / first_ignore_nulls() aggregate for PySpark parity.
Sparkless test_first_ignorenulls and test_first_method are skipped.

Run from repo root: python scripts/robin_parity_repros/26_first_ignore_nulls.py

PySpark equivalent:
  df.groupBy("k").agg(F.first("v", ignorenulls=True).alias("first_v"))
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
    spark = rs.SparkSession.builder().app_name("repro-first").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"k": "a", "v": 1}, {"k": "a", "v": None}, {"k": "a", "v": 3}],
        [("k", "string"), ("v", "int")],
    )
    gb = getattr(df, "group_by", getattr(df, "groupBy", None))
    if gb is None:
        return False, "DataFrame has no group_by/groupBy"
    gd = gb("k") if callable(gb) else gb(["k"])
    first_fn = getattr(rs, "first", None)
    if not first_fn:
        return False, "robin_sparkless has no 'first' function"
    try:
        # first(col, ignorenulls=True)
        agg_expr = first_fn(rs.col("v"), ignorenulls=True)
        if hasattr(agg_expr, "alias"):
            agg_expr = agg_expr.alias("first_v")
        out = gd.agg([agg_expr])
        out.collect()
    except TypeError as e:
        if "ignorenulls" in str(e).lower():
            return False, f"first(ignorenulls=...): {e}"
        return False, f"first(): {e}"
    except Exception as e:
        return False, f"groupBy.agg(first): {type(e).__name__}: {e}"
    return True, "first(ignorenulls=True) OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", 1), ("a", None), ("a", 3)], ["k", "v"])
        df.groupBy("k").agg(F.first("v", ignorenulls=True).alias("first_v")).collect()
        spark.stop()
        return True, "PySpark first(ignorenulls=True) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
