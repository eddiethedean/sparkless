#!/usr/bin/env python3
"""
Repro: SparkSession.sql() and SparkSession.table() for PySpark parity.
Sparkless tests skip when spark.sql() or spark.table() are not available.

Run from repo root: python scripts/robin_parity_repros/19_spark_sql_table.py

PySpark equivalent:
  spark = SparkSession.builder.master("local").getOrCreate()
  df = spark.sql("SELECT 1 as one")
  df2 = spark.table("my_table")  # after createOrReplaceTempView
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
    spark = rs.SparkSession.builder().app_name("repro-sql-table").get_or_create()
    # sql(query)
    if not hasattr(spark, "sql"):
        return False, "SparkSession has no attribute 'sql'"
    try:
        df = spark.sql("SELECT 1 as one")
        rows = df.collect()
        if not rows:
            return False, "sql() returned empty"
    except Exception as e:
        return False, f"spark.sql(): {type(e).__name__}: {e}"
    # table(name)
    if not hasattr(spark, "table"):
        return False, "SparkSession has no attribute 'table'"
    try:
        _ = spark.table("nonexistent")  # may raise or return empty
    except NotImplementedError as e:
        return False, f"spark.table(): {e}"
    except Exception as e:
        pass  # table not found is ok for this repro
    return True, "sql() and table() present"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.sql("SELECT 1 as one")
        rows = df.collect()
        spark.stop()
        return True, f"PySpark sql() OK: {len(rows)} row(s)"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
