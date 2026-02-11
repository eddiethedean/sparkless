#!/usr/bin/env python3
"""
Limitation: concat with literal in middle — concat(col1, lit(' '), col2) may
raise "not found: concat(first_name,  , last_name)".

Run from repo root: python scripts/repro_robin_limitations/08_concat_literal.py
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
    if not hasattr(F, "concat"):
        ROBIN_FAIL.append("F.concat not found")
        return
    spark = F.SparkSession.builder().app_name("repro-concat").get_or_create()
    data = [{"first_name": "Alice", "last_name": "Smith"}, {"first_name": "Bob", "last_name": "Jones"}]
    schema = [("first_name", "string"), ("last_name", "string")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    try:
        expr = F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
        out = df.with_column("full_name", expr).collect()
        ROBIN_OK.append(f"with_column(concat(..., lit(' '), ...)): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"with_column(concat with lit): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame(
            [{"first_name": "Alice", "last_name": "Smith"}, {"first_name": "Bob", "last_name": "Jones"}]
        )
        out = df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))).collect()
        PYSPARK_OK.append(f"withColumn(concat with lit): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("08_concat_literal: withColumn(concat(col1, lit(' '), col2))")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
