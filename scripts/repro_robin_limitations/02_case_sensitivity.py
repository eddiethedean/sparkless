#!/usr/bin/env python3
"""
Limitation: Column name case sensitivity — Robin may be case-sensitive;
PySpark default is case-insensitive ("Name" vs "name").

Run from repo root: python scripts/repro_robin_limitations/02_case_sensitivity.py
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
    spark = F.SparkSession.builder().app_name("repro-case").get_or_create()
    data = [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
    schema = [("Name", "string"), ("Age", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    # Use lowercase "name" to select; column is "Name"
    try:
        out = df.select(["name"]).collect()
        ROBIN_OK.append(f"select(['name']) returned {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"select(['name']) (column is 'Name'): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}])
        out = df.select("name").collect()
        PYSPARK_OK.append(f"select('name') returned {len(out)} rows (case-insensitive)")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("02_case_sensitivity: select('name') when column is 'Name'")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
