#!/usr/bin/env python3
"""
Verify: orderBy / order_by with nulls_first, nulls_last.
Sparkless fails with "Operation 'Operations: orderBy' is not supported".
This script tests direct Robin API to determine if it's Robin or Sparkless.

Run from repo root: python scripts/repro_robin_limitations/11_order_by_nulls.py
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
    spark = F.SparkSession.builder().app_name("repro-order-by-nulls").get_or_create()
    data = [{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}, {"value": "D"}]
    schema = [("value", "string")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(data, schema)

    # PySpark uses orderBy(col.desc_nulls_last()). Robin uses order_by(cols, ascending=[...]).
    # Test: does Robin Column have desc_nulls_last for PySpark parity?
    col_obj = F.col("value")
    desc_nulls_last = getattr(col_obj, "desc_nulls_last", None)
    if desc_nulls_last is None:
        ROBIN_FAIL.append("col.desc_nulls_last not found (PySpark parity)")
        return
    try:
        # Robin order_by may accept Column or only column names; try both
        expr = desc_nulls_last() if callable(desc_nulls_last) else desc_nulls_last
        out = df.order_by(expr).collect() if hasattr(df, "order_by") else df.orderBy(expr).collect()
        ROBIN_OK.append(f"order_by(col.desc_nulls_last()): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"order_by(col.desc_nulls_last()): {type(e).__name__}: {e}")


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
            [{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}, {"value": "D"}]
        )
        out = df.orderBy(F.col("value").desc_nulls_last()).collect()
        PYSPARK_OK.append(f"orderBy(desc_nulls_last): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("11_order_by_nulls: orderBy with desc_nulls_last")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
