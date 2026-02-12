#!/usr/bin/env python3
"""
Repro: select with aliased expression. Sparkless fails with "Operation 'Operations: select'
is not supported" or "Column 'X' not found" when alias is used; verify Robin preserves
output column name as alias.

Run from repo root: python scripts/robin_parity_repros/03_select_alias.py
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
    spark = F.SparkSession.builder().app_name("repro-select-alias").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df([{"a": 1, "b": 2}], [("a", "int"), ("b", "int")])
    try:
        out = df.select(F.col("a").alias("a_as_int")).collect()
        if out and hasattr(out[0], "_fields") and "a_as_int" in out[0]._fields:
            ROBIN_OK.append(f"select(alias): columns={out[0]._fields}")
        elif out and isinstance(out[0], dict) and "a_as_int" in out[0]:
            ROBIN_OK.append(f"select(alias): columns={list(out[0].keys())}")
        else:
            row0 = out[0]
            cols = getattr(row0, "_fields", list(row0.keys()) if isinstance(row0, dict) else [])
            ROBIN_FAIL.append(f"select(alias): wrong columns: {cols}")
    except Exception as e:
        ROBIN_FAIL.append(f"select(alias): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"a": 1, "b": 2}])
        out = df.select(F.col("a").alias("a_as_int")).collect()
        row = out[0]
        names = row.asDict().keys() if hasattr(row, "asDict") else row._fields
        PYSPARK_OK.append(f"select(alias): columns={list(names)}")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("03_select_alias: select with aliased expression (output column name)")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
