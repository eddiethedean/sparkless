#!/usr/bin/env python3
"""
Repro: explode_outer — PySpark parity. Like explode but produces one row with NULLs if input is NULL/empty.
Run from repo root: python scripts/robin_parity_repros/37_explode_outer.py
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
    spark = rs.SparkSession.builder().app_name("repro-explode_outer").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"name": "a", "arr": [1, 2]}, {"name": "b", "arr": None}]
    df = create_df(data, [("name", "string"), ("arr", "array")])
    fn = getattr(rs, "explode_outer", None)
    if not fn:
        return False, "robin_sparkless has no explode_outer"
    try:
        out = df.select("name", fn(rs.col("arr")).alias("v"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        return True, "explode_outer OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, IntegerType
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame(
            [("a", [1, 2]), ("b", None)],
            "name string, arr array<int>"
        )
        out = df.select("name", F.explode_outer("arr").alias("v"))
        rows = out.collect()
        spark.stop()
        if len(rows) >= 2:
            return True, "PySpark explode_outer OK"
        return False, f"PySpark got {len(rows)} rows"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
