#!/usr/bin/env python3
"""Reproduce robin-sparkless unsupported expression ops vs PySpark parity.

Robin fails for: concat, contains (and possibly others)
PySpark supports these.

Usage:
  python scripts/repro_robin_issue_unsupported_ops.py robin
  python scripts/repro_robin_issue_unsupported_ops.py pyspark
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin():
    from sparkless.sql import SparkSession
    from sparkless.sql import functions as F

    spark = SparkSession.builder.appName("repro").getOrCreate()

    # concat
    try:
        df = spark.createDataFrame([{"a": "x", "b": "y"}])
        r = df.select(F.concat(F.col("a"), F.col("b")).alias("c")).collect()
        print("Robin concat: OK", r[0]["c"])
    except Exception as e:
        print("Robin concat: FAIL", type(e).__name__, str(e)[:80])

    # contains
    try:
        df = spark.createDataFrame([{"name": "Alice"}])
        r = df.filter(F.col("name").contains("lic")).collect()
        print("Robin contains: OK", len(r))
    except Exception as e:
        print("Robin contains: FAIL", type(e).__name__, str(e)[:80])

    spark.stop()


def run_pyspark():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("repro").getOrCreate()

    df = spark.createDataFrame([{"a": "x", "b": "y"}])
    r = df.select(F.concat(F.col("a"), F.col("b")).alias("c")).collect()
    assert r[0]["c"] == "xy"
    print("PySpark concat: OK", r[0]["c"])

    df = spark.createDataFrame([{"name": "Alice"}])
    r = df.filter(F.col("name").contains("lic")).collect()
    assert len(r) == 1
    print("PySpark contains: OK", len(r))

    spark.stop()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "robin"
    if mode == "robin":
        run_robin()
    else:
        run_pyspark()
