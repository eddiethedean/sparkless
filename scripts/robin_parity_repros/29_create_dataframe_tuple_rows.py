#!/usr/bin/env python3
"""
Repro: createDataFrame(list_of_tuples, schema) — PySpark parity.
#256 covers array/list *column type*; this is positional rows (tuples/lists) with schema.

Run from repo root: python scripts/robin_parity_repros/29_create_dataframe_tuple_rows.py

PySpark equivalent:
  data = [("Alice", 1), ("Bob", 2)]
  schema = StructType([StructField("name", StringType()), StructField("value", IntegerType())])
  df = spark.createDataFrame(data, schema)
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
    spark = rs.SparkSession.builder().app_name("repro-tuple-rows").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    # PySpark: createDataFrame([("Alice", 1), ("Bob", 2)], schema)
    data = [("Alice", 1), ("Bob", 2)]
    schema = [("name", "string"), ("value", "int")]
    try:
        df = create_df(data, schema)
        rows = df.collect()
        if len(rows) != 2:
            return False, f"Robin returned {len(rows)} rows, expected 2"
        # Check first row (may be dict or Row-like)
        r0 = rows[0]
        name = r0.get("name") if isinstance(r0, dict) else getattr(r0, "name", None)
        value = r0.get("value") if isinstance(r0, dict) else getattr(r0, "value", None)
        if name != "Alice" or value != 1:
            return False, f"Robin first row: name={name!r}, value={value!r}"
        try:
            spark.stop()
        except Exception:
            pass
        return True, "create_dataframe_from_rows(tuple_rows, schema) OK"
    except Exception as e:
        return False, f"Robin: {type(e).__name__}: {e}"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        data = [("Alice", 1), ("Bob", 2)]
        schema = StructType([
            StructField("name", StringType()),
            StructField("value", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        rows = df.collect()
        spark.stop()
        if len(rows) != 2:
            return False, f"PySpark returned {len(rows)} rows"
        if rows[0]["name"] != "Alice" or rows[0]["value"] != 1:
            return False, f"PySpark first row: {rows[0]}"
        return True, "PySpark createDataFrame(tuple_rows, schema) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
