#!/usr/bin/env python3
"""
Repro: inline(array of structs) — PySpark parity. Explodes array of structs into table.
Run from repo root: python scripts/robin_parity_repros/38_inline.py
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
    spark = rs.SparkSession.builder().app_name("repro-inline").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    # Array of structs: Robin may use different schema syntax
    data = [{"id": 1, "items": [{"x": 10, "y": "a"}, {"x": 20, "y": "b"}]}]
    try:
        df = create_df(data, [("id", "int"), ("items", "array")])
    except Exception as e:
        return False, f"create_df array of structs: {e}"
    fn = getattr(rs, "inline", None)
    if not fn:
        return False, "robin_sparkless has no inline"
    try:
        out = df.select(fn(rs.col("items")))
        out.collect()
        return True, "inline OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("items", ArrayType(StructType([
                StructField("x", IntegerType()),
                StructField("y", StringType()),
            ]))),
        ])
        df = spark.createDataFrame([(1, [{"x": 10, "y": "a"}, {"x": 20, "y": "b"}])], schema)
        out = df.select(F.inline("items"))
        rows = out.collect()
        spark.stop()
        if len(rows) == 2:
            return True, "PySpark inline OK"
        return False, f"PySpark got {len(rows)} rows, expected 2"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
