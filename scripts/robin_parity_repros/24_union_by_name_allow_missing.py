#!/usr/bin/env python3
"""
Repro: union_by_name(other, allow_missing_columns=True) for PySpark parity.
Sparkless compat layer tries allow_missing_columns; Robin may not support it.

Run from repo root: python scripts/robin_parity_repros/24_union_by_name_allow_missing.py

PySpark equivalent:
  df1.unionByName(df2, allowMissingColumns=True)
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
    spark = rs.SparkSession.builder().app_name("repro-union").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df1 = create_df([{"a": 1, "b": 2}], [("a", "int"), ("b", "int")])
    df2 = create_df([{"a": 3, "c": 4}], [("a", "int"), ("c", "int")])  # different cols
    fn = getattr(df1, "union_by_name", None)
    if not fn:
        return False, "DataFrame has no attribute 'union_by_name'"
    try:
        out = fn(df2, allow_missing_columns=True)
        out.collect()
    except TypeError as e:
        if "allow_missing" in str(e).lower() or "unexpected keyword" in str(e).lower():
            return False, f"union_by_name(allow_missing_columns=True): {e}"
        raise
    except Exception as e:
        return False, f"union_by_name: {type(e).__name__}: {e}"
    return True, "union_by_name(allow_missing_columns=True) OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df1 = spark.createDataFrame([(1, 2)], ["a", "b"])
        df2 = spark.createDataFrame([(3, 4)], ["a", "c"])
        df1.unionByName(df2, allowMissingColumns=True).collect()
        spark.stop()
        return True, "PySpark unionByName(allowMissingColumns=True) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
