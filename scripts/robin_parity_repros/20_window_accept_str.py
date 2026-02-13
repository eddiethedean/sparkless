#!/usr/bin/env python3
"""
Repro: Window.partitionBy() / orderBy() should accept column names (str) for PySpark parity.
Sparkless window tests skip when Window expects Column and not str.

Run from repo root: python scripts/robin_parity_repros/20_window_accept_str.py

PySpark equivalent:
  from pyspark.sql import Window
  w = Window.partitionBy("dept").orderBy("salary")
  df.select(F.col("dept"), F.col("salary"), F.row_number().over(w).alias("rn"))
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
    spark = rs.SparkSession.builder().app_name("repro-window-str").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"dept": "A", "salary": 100}, {"dept": "A", "salary": 200}],
        [("dept", "string"), ("salary", "int")],
    )
    try:
        w = rs.Window.partition_by("dept").order_by("salary")
    except Exception as e:
        return False, f"Window.partition_by/order_by(str): {type(e).__name__}: {e}"
    # Try partitionBy/orderBy (camelCase) if that's the API
    try:
        w2 = getattr(rs.Window, "partitionBy", rs.Window.partition_by)("dept")
        w2 = getattr(w2, "orderBy", getattr(w2, "order_by", lambda x: None))("salary")
    except Exception as e:
        return False, f"Window.partitionBy/orderBy(str): {type(e).__name__}: {e}"
    return True, "Window accepted string column names"


def run_pyspark() -> tuple[bool, str]:
    try:
        from pyspark.sql import SparkSession, Window
        spark = SparkSession.builder.master("local[1]").getOrCreate()
        df = spark.createDataFrame([("A", 100), ("A", 200)], ["dept", "salary"])
        w = Window.partitionBy("dept").orderBy("salary")
        spark.stop()
        return True, "PySpark Window.partitionBy/orderBy(str) OK"
    except Exception as e:
        return False, f"PySpark: {type(e).__name__}: {e}"


if __name__ == "__main__":
    r_ok, r_msg = run_robin()
    p_ok, p_msg = run_pyspark()
    print("Robin:", "PASS" if r_ok else "FAIL", r_msg)
    print("PySpark:", "PASS" if p_ok else "FAIL", p_msg)
    sys.exit(0 if (r_ok and p_ok) else 1)
