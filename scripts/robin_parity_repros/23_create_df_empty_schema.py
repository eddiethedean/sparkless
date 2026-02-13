#!/usr/bin/env python3
"""
Repro: create_dataframe_from_rows with empty data + schema or empty schema (PySpark parity).
Sparkless tests fail with "schema must not be empty" or reject empty data.

Run from repo root: python scripts/robin_parity_repros/23_create_df_empty_schema.py

PySpark equivalent:
  spark.createDataFrame([], "a: int, b: string")
  spark.createDataFrame([], schema)  # StructType with fields
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
    spark = rs.SparkSession.builder().app_name("repro-empty-df").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    # Empty data with non-empty schema
    try:
        df = create_df([], [("a", "int"), ("b", "string")])
        if df is None:
            return False, "create_dataframe_from_rows([], schema) returned None"
        n = len(df.collect()) if hasattr(df, "collect") else 0
        return True, f"create_dataframe_from_rows([], schema) OK: {n} rows"
    except Exception as e:
        return False, f"create_dataframe_from_rows([], schema): {type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
        df = spark.createDataFrame([], schema)
        n = df.count()
        spark.stop()
        return True, f"PySpark createDataFrame([], schema) OK: {n} rows"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
