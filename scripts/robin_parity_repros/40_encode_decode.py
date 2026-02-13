#!/usr/bin/env python3
"""
Repro: encode(col, charset) / decode(col, charset) — PySpark string parity.
Run from repo root: python scripts/robin_parity_repros/40_encode_decode.py
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
    spark = rs.SparkSession.builder().app_name("repro-encode").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"s": "hello"}]
    df = create_df(data, [("s", "string")])
    encode = getattr(rs, "encode", None)
    if not encode:
        return False, "robin_sparkless has no encode"
    try:
        out = df.select(encode(rs.col("s"), "UTF-8").alias("e"))
        out.collect()
        return True, "encode/decode OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("hello",)], ["s"])
        out = df.select(F.encode("s", "UTF-8").alias("e"))
        rows = out.collect()
        spark.stop()
        return True, "PySpark encode OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
