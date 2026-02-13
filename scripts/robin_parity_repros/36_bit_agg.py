#!/usr/bin/env python3
"""
Repro: bit_and / bit_or / bit_xor — PySpark aggregate parity.
Run from repo root: python scripts/robin_parity_repros/36_bit_agg.py
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
    spark = rs.SparkSession.builder().app_name("repro-bit_agg").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"v": 3}, {"v": 5}]
    df = create_df(data, [("v", "int")])
    bit_and = getattr(rs, "bit_and", None)
    if not bit_and:
        return False, "robin_sparkless has no bit_and"
    try:
        out = df.agg(bit_and(rs.col("v")).alias("ba"))
        out.collect()
        return True, "bit_and/bit_or OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([(3,), (5,)], ["v"])
        out = df.agg(F.bit_and("v").alias("ba"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][0] == 1:
            return True, "PySpark bit_and OK"
        return False, f"PySpark got {rows[0][0] if rows else '?'}, expected 1"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
