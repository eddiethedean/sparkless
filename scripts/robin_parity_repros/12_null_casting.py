#!/usr/bin/env python3
"""
Repro: casting null literal to various types. PySpark accepts lit(None).cast(type);
Robin may raise "casting from null to X not supported".

Run from repo root: python scripts/robin_parity_repros/12_null_casting.py
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
    spark = F.SparkSession.builder().app_name("repro-null-cast").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df([{"id": 1}], [("id", "int")])
    try:
        out = df.select(F.lit(None).cast("double").alias("null_double")).collect()
        ROBIN_OK.append(f"lit(None).cast(double): {out[0]['null_double']}")
    except Exception as e:
        ROBIN_FAIL.append(f"lit(None).cast(double): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"id": 1}])
        out = df.select(F.lit(None).cast("double").alias("null_double")).collect()
        PYSPARK_OK.append(f"lit(None).cast(double): {out[0]['null_double']}")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("12_null_casting: lit(None).cast(type)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    if ROBIN_FAIL and PYSPARK_OK:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
