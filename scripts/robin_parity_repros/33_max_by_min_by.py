#!/usr/bin/env python3
"""
Repro: max_by(x, y) / min_by(x, y) — PySpark parity. Value of x where y is max/min.
Run from repo root: python scripts/robin_parity_repros/33_max_by_min_by.py
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
    spark = rs.SparkSession.builder().app_name("repro-max_by").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"name": "a", "score": 10}, {"name": "b", "score": 30}, {"name": "c", "score": 20}]
    df = create_df(data, [("name", "string"), ("score", "int")])
    max_by = getattr(rs, "max_by", None)
    if not max_by:
        return False, "robin_sparkless has no max_by"
    try:
        out = df.agg(max_by(rs.col("name"), rs.col("score")).alias("top"))
        rows = out.collect()
        if not rows:
            return False, "no rows"
        val = rows[0].get("top") or (list(rows[0].values())[0] if rows[0] else None)
        if val != "b":
            return False, f"max_by got {val!r}, expected 'b'"
        return True, "max_by/min_by OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("a", 10), ("b", 30), ("c", 20)], ["name", "score"])
        out = df.agg(F.max_by("name", "score").alias("top"))
        rows = out.collect()
        spark.stop()
        if rows and rows[0][0] == "b":
            return True, "PySpark max_by OK"
        return False, f"PySpark got {rows[0][0] if rows else '?'}, expected 'b'"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
