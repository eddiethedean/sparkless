#!/usr/bin/env python3
"""
Limitation: Chained arithmetic in select/withColumn — e.g. (col("a")*2 + col("b"))
or (col("id") % 2) may fail with "not found" or type error.

Run from repo root: python scripts/repro_robin_limitations/09_chained_arithmetic.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ROBIN_OK: list[str] = []
ROBIN_FAIL: list[str] = []
PYSPARK_OK: list[str] = []
PYSPARK_SKIP: list[str] = []


def run_robin() -> None:
    try:
        import robin_sparkless as rs
    except ImportError as e:
        ROBIN_FAIL.append(f"robin_sparkless not installed: {e}")
        return
    F = rs
    spark = F.SparkSession.builder().app_name("repro-arithmetic").get_or_create()
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = [("a", "int"), ("b", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    try:
        expr = (F.col("a") * F.lit(2)) + F.col("b")
        out = df.with_column("sum", expr).collect()
        ROBIN_OK.append(f"with_column((a*2)+b): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"with_column((a*2)+b): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        out = df.withColumn("sum", (F.col("a") * 2) + F.col("b")).collect()
        PYSPARK_OK.append(f"withColumn((a*2)+b): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("09_chained_arithmetic: withColumn((col('a')*2) + col('b'))")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
