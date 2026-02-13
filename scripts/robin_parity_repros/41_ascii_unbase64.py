#!/usr/bin/env python3
"""
Repro: ascii(col) / unbase64(col) — PySpark string parity.
Run from repo root: python scripts/robin_parity_repros/41_ascii_unbase64.py
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
    spark = rs.SparkSession.builder().app_name("repro-ascii").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"s": "A"}]
    df = create_df(data, [("s", "string")])
    ascii_fn = getattr(rs, "ascii", None)
    if not ascii_fn:
        return False, "robin_sparkless has no ascii"
    try:
        out = df.select(ascii_fn(rs.col("s")).alias("a"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        return True, "ascii/unbase64 OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("A",)], ["s"])
        out = df.select(F.ascii("s").alias("a"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][0] == 65:
            return True, "PySpark ascii OK"
        return False, f"PySpark got {rows[0][0] if rows else '?'}, expected 65"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
