#!/usr/bin/env python3
"""
Limitation: Window in select — row_number().over(Window.partitionBy().orderBy())
in select or withColumn may not be supported.

Run from repo root: python scripts/repro_robin_limitations/05_window_in_select.py
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
    spark = F.SparkSession.builder().app_name("repro-window").get_or_create()
    data = [{"dept": "A", "salary": 10}, {"dept": "A", "salary": 20}, {"dept": "B", "salary": 15}]
    schema = [("dept", "string"), ("salary", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    if not hasattr(F, "row_number") or not hasattr(F, "Window"):
        ROBIN_FAIL.append("row_number or Window not found")
        return
    try:
        win = F.Window.partition_by("dept").order_by(F.col("salary"))
        out = df.with_column("rn", F.row_number().over(win)).collect()
        ROBIN_OK.append(f"with_column(row_number().over(...)): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"with_column(row_number().over(...)): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame(
            [{"dept": "A", "salary": 10}, {"dept": "A", "salary": 20}, {"dept": "B", "salary": 15}]
        )
        win = Window.partitionBy("dept").orderBy(F.col("salary"))
        out = df.withColumn("rn", F.row_number().over(win)).collect()
        PYSPARK_OK.append(f"withColumn(row_number().over(...)): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("05_window_in_select: withColumn(row_number().over(Window.partitionBy(...)))")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
