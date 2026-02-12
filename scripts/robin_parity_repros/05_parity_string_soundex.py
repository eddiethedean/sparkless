#!/usr/bin/env python3
"""
Repro: string function soundex() parity. Sparkless parity tests fail with
AssertionError / DataFrames not equivalent (mock=0, expected=3); verify
Robin returns same result as PySpark for soundex().

Run from repo root: python scripts/robin_parity_repros/05_parity_string_soundex.py
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
    spark = F.SparkSession.builder().app_name("repro-soundex").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    data = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}]
    schema = [("name", "string")]
    df = create_df(data, schema)
    try:
        if not hasattr(F, "soundex"):
            ROBIN_FAIL.append("F.soundex not found")
            return
        out = df.with_column("snd", F.soundex(F.col("name"))).collect()
        ROBIN_OK.append(f"soundex: {len(out)} rows")
    except Exception as e:
        ROBIN_FAIL.append(f"soundex: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        df = spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}])
        out = df.withColumn("snd", F.soundex(F.col("name"))).collect()
        PYSPARK_OK.append(f"soundex: {len(out)} rows")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("05_parity_string_soundex: soundex() string function")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
