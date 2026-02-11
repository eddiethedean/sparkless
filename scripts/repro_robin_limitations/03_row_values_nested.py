#!/usr/bin/env python3
"""
Limitation: Row values — Robin create_dataframe_from_rows may only accept
None, int, float, bool, str; nested list/dict raise.

Run from repo root: python scripts/repro_robin_limitations/03_row_values_nested.py
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
    spark = rs.SparkSession.builder().app_name("repro-nested").get_or_create()
    # Row with list value
    data = [{"id": 1, "tags": [1, 2, 3]}, {"id": 2, "tags": [4, 5]}]
    schema = [("id", "int"), ("tags", "array<int>")]
    try:
        create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
        df = create_df(data, schema)
        out = df.collect()
        ROBIN_OK.append(f"create_dataframe_from_rows with list column: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"create_dataframe_from_rows with list: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("tags", ArrayType(IntegerType())),
        ])
        df = spark.createDataFrame([(1, [1, 2, 3]), (2, [4, 5])], schema)
        out = df.collect()
        PYSPARK_OK.append(f"createDataFrame with array: {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("03_row_values_nested: create_dataframe_from_rows with list column")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
