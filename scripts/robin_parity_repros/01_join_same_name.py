#!/usr/bin/env python3
"""
Repro: Join with same-name key. PySpark accepts on="id" (string); Robin may require on=["id"].

Run from repo root: python scripts/robin_parity_repros/01_join_same_name.py
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


def run_robin_list() -> None:
    """Robin: join with on=[\"id\"] (list)."""
    try:
        import robin_sparkless as rs
    except ImportError as e:
        ROBIN_FAIL.append(f"robin_sparkless not installed: {e}")
        return
    F = rs
    spark = F.SparkSession.builder().app_name("repro-join").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df1 = create_df([{"id": 1, "x": 10}, {"id": 2, "x": 20}], [("id", "bigint"), ("x", "bigint")])
    df2 = create_df([{"id": 1, "y": 100}, {"id": 2, "y": 200}], [("id", "bigint"), ("y", "bigint")])
    try:
        result = df1.join(df2, on=["id"], how="inner")
        out = result.collect()
        ROBIN_OK.append(f"join(on=['id'], how='inner'): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"join(on=['id']): {type(e).__name__}: {e}")


def run_robin_string() -> None:
    """Robin: join with on=\"id\" (string) - PySpark accepts this."""
    try:
        import robin_sparkless as rs
    except ImportError:
        return
    F = rs
    spark = F.SparkSession.builder().app_name("repro-join").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df1 = create_df([{"id": 1, "x": 10}], [("id", "bigint"), ("x", "bigint")])
    df2 = create_df([{"id": 1, "y": 100}], [("id", "bigint"), ("y", "bigint")])
    try:
        result = df1.join(df2, on="id", how="inner")
        out = result.collect()
        ROBIN_OK.append(f"join(on='id', how='inner'): {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"join(on='id'): {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df1 = spark.createDataFrame([{"id": 1, "x": 10}, {"id": 2, "x": 20}])
        df2 = spark.createDataFrame([{"id": 1, "y": 100}, {"id": 2, "y": 200}])
        out1 = df1.join(df2, on="id", how="inner").collect()
        out2 = df1.join(df2, on=["id"], how="inner").collect()
        PYSPARK_OK.append(f"join(on='id'): {len(out1)} rows; join(on=['id']): {len(out2)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("01_join_same_name: inner join on same-named column (on='id' vs on=['id'])")
    run_robin_list()
    run_robin_string()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
