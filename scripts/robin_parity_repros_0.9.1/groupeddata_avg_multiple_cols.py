#!/usr/bin/env python3
"""
Repro: GroupedData.avg() with multiple column names (PySpark: df.groupBy("x").avg("a", "b")).
Robin 0.9.1: GroupedData.avg() takes 1 positional argument but 2 were given.

Run from repo root: python scripts/robin_parity_repros_0.9.1/groupeddata_avg_multiple_cols.py
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
    spark = F.SparkSession.builder().app_name("repro-avg").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"dept": "A", "salary": 100, "bonus": 10}, {"dept": "A", "salary": 200, "bonus": 20}, {"dept": "B", "salary": 150, "bonus": 15}],
        [("dept", "string"), ("salary", "int"), ("bonus", "int")],
    )
    try:
        g = df.group_by("dept")
        out = g.avg("salary", "bonus").collect()
        ROBIN_OK.append(f"groupBy.avg(multi): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"groupBy.avg(multi): {type(e).__name__}: {e}")


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
            [{"dept": "A", "salary": 100, "bonus": 10}, {"dept": "A", "salary": 200, "bonus": 20}, {"dept": "B", "salary": 150, "bonus": 15}]
        )
        out = df.groupBy("dept").avg("salary", "bonus").collect()
        PYSPARK_OK.append(f"groupBy.avg(multi): {len(out)} rows, e.g. {out[0]}")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark error: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("groupeddata_avg_multiple_cols: df.groupBy('dept').avg('salary', 'bonus')")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
