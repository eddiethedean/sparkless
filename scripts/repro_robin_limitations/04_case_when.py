#!/usr/bin/env python3
"""
Limitation: CaseWhen (when/otherwise) in select or withColumn may not be
supported or may resolve as "not found".

Run from repo root: python scripts/repro_robin_limitations/04_case_when.py
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
    spark = F.SparkSession.builder().app_name("repro-casewhen").get_or_create()
    data = [{"a": 1}, {"a": -1}, {"a": 0}]
    schema = [("a", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    if not hasattr(F, "when"):
        ROBIN_FAIL.append("F.when not found")
        return
    try:
        expr = F.when(F.col("a").gt(F.lit(0)), F.lit(1)).otherwise(F.lit(0))
        out = df.with_column("x", expr).collect()
        ROBIN_OK.append(f"with_column(when(a>0, 1).otherwise(0)): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"with_column(when/otherwise): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"a": 1}, {"a": -1}, {"a": 0}])
        out = df.withColumn("x", F.when(F.col("a") > 0, F.lit(1)).otherwise(F.lit(0))).collect()
        PYSPARK_OK.append(f"withColumn(when/otherwise): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("04_case_when: withColumn with when(a>0, 1).otherwise(0)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
