#!/usr/bin/env python3
"""
Repro: DataFrame.fillna(value, subset=[...]) for PySpark parity.
Sparkless tests fail with "DataFrame.fillna() got an unexpected keyword argument 'subset'".

Run from repo root: python scripts/robin_parity_repros/22_fillna_subset.py

PySpark equivalent:
  df.fillna(0, subset=["col1", "col2"])
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
    spark = rs.SparkSession.builder().app_name("repro-fillna").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"a": 1, "b": None}, {"a": None, "b": 2}],
        [("a", "int"), ("b", "int")],
    )
    if not hasattr(df, "fillna"):
        return False, "DataFrame has no attribute 'fillna'"
    try:
        out = df.fillna(0, subset=["b"])
        out.collect()
    except TypeError as e:
        if "subset" in str(e) or "unexpected keyword" in str(e).lower():
            return False, f"fillna(subset=...): {e}"
        raise
    except Exception as e:
        return False, f"fillna: {type(e).__name__}: {e}"
    return True, "fillna(subset=...) OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(1, None), (None, 2)], ["a", "b"])
        df.fillna(0, subset=["b"]).collect()
        spark.stop()
        return True, "PySpark fillna(subset=...) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
