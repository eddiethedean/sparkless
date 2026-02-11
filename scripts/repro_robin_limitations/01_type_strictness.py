#!/usr/bin/env python3
"""
Limitation: Type strictness — Robin does not allow comparing string with numeric.
PySpark coerces (e.g. string column == int literal); Robin may raise.

Run from repo root: python scripts/repro_robin_limitations/01_type_strictness.py
Requires: pip install robin-sparkless>=0.6.0; PySpark optional for baseline.
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
    spark = F.SparkSession.builder().app_name("repro-type-strictness").get_or_create()
    # DataFrame: str_col has string "123"; compare to int 123
    data = [{"str_col": "123"}, {"str_col": "456"}]
    schema = [("str_col", "string")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    try:
        out = df.filter(F.col("str_col") == F.lit(123)).collect()
        ROBIN_OK.append(f"filter str_col==123 returned {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"filter str_col==123: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"str_col": "123"}, {"str_col": "456"}])
        out = df.filter(F.col("str_col") == F.lit(123)).collect()
        PYSPARK_OK.append(f"filter str_col==123 returned {len(out)} rows (PySpark coerces)")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark raised: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("01_type_strictness: string column compared to int literal")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
