#!/usr/bin/env python3
"""
Limitation: map() / array() — Robin may not expose F.map() or F.array() or
may have different API ("not found: map()").

Run from repo root: python scripts/repro_robin_limitations/06_map_array.py
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
    if not hasattr(F, "map") and not hasattr(F, "array"):
        ROBIN_FAIL.append("F.map and F.array not found")
        return
    spark = F.SparkSession.builder().app_name("repro-map-array").get_or_create()
    data = [{"k": "a", "v": 1}, {"k": "b", "v": 2}]
    schema = [("k", "string"), ("v", "int")]
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
    df = create_df(data, schema)
    try:
        if hasattr(F, "map"):
            expr = F.map(F.col("k"), F.col("v"))
        else:
            expr = F.array(F.col("k"), F.col("v"))
        out = df.with_column("m", expr).collect()
        ROBIN_OK.append(f"with_column(map/array): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"with_column(map/array): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"k": "a", "v": 1}, {"k": "b", "v": 2}])
        out = df.withColumn("m", F.map_from_arrays(F.array(F.col("k")), F.array(F.col("v")))).collect()
        PYSPARK_OK.append(f"withColumn(map): {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("06_map_array: withColumn with map() or array() expression")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
