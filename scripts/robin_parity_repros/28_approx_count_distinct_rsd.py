#!/usr/bin/env python3
"""
Repro: approx_count_distinct(column, rsd=...) for PySpark parity.
Skip list: test_approx_count_distinct_rsd; doc says "Check; file if missing".

Run from repo root: python scripts/robin_parity_repros/28_approx_count_distinct_rsd.py

PySpark equivalent:
  df.agg(F.approx_count_distinct("value", rsd=0.01))
  df.groupBy("k").agg(F.approx_count_distinct("value", rsd=0.05))
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
    spark = rs.SparkSession.builder().app_name("repro-approx").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [
        {"k": "A", "value": 1},
        {"k": "A", "value": 10},
        {"k": "A", "value": 1},
        {"k": "B", "value": 5},
        {"k": "B", "value": 5},
    ]
    df = create_df(data, [("k", "string"), ("value", "int")])
    approx = getattr(rs, "approx_count_distinct", None)
    if not approx:
        return False, "robin_sparkless has no approx_count_distinct"
    try:
        # Global agg with rsd
        out = df.agg(approx(rs.col("value"), rsd=0.01))
        rows = out.collect()
        if not rows:
            return False, "Robin agg returned no rows"
        # Expect distinct count 3 (1, 10, 5)
        val = rows[0].get("approx_count_distinct(value)") or rows[0].get(list(rows[0].keys())[0])
        if val != 3:
            return False, f"Robin approx_count_distinct(rsd=0.01) got {val}, expected 3"
    except TypeError as e:
        if "rsd" in str(e).lower() or "keyword" in str(e).lower():
            return False, f"Robin approx_count_distinct does not accept rsd: {e}"
        raise
    except Exception as e:
        return False, f"Robin: {type(e).__name__}: {e}"
    try:
        spark.stop()
    except Exception:
        pass
    return True, "approx_count_distinct(rsd=0.01) OK"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame(
            [("A", 1), ("A", 10), ("A", 1), ("B", 5), ("B", 5)], ["k", "value"]
        )
        out = df.agg(F.approx_count_distinct("value", rsd=0.01))
        rows = out.collect()
        spark.stop()
        if not rows:
            return False, "PySpark returned no rows"
        val = rows[0][0]
        if val != 3:
            return False, f"PySpark got {val}, expected 3"
        return True, "PySpark approx_count_distinct(rsd=0.01) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
