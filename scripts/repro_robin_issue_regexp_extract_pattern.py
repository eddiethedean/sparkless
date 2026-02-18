#!/usr/bin/env python3
"""Reproduce robin-sparkless regexp_extract literal pattern vs PySpark parity.

Robin fails: regexp_extract in plan requires literal pattern at arg 2 (column refs not supported)
PySpark: F.regexp_extract(F.col("s"), r"(\\w+)", 1) works - pattern is literal.

Usage:
  python scripts/repro_robin_issue_regexp_extract_pattern.py robin
  python scripts/repro_robin_issue_regexp_extract_pattern.py pyspark
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
    df = spark.createDataFrame([{"s": "hello world"}])
    try:
        result = df.select(
            F.regexp_extract(F.col("s"), r"(\w+)", 1).alias("first")
        ).collect()
        print("Robin: OK", result[0]["first"])
    except Exception as e:
        print("Robin: FAIL", type(e).__name__, str(e))
    spark.stop()


def run_pyspark():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("repro").getOrCreate()
    df = spark.createDataFrame([{"s": "hello world"}])
    result = df.select(
        F.regexp_extract(F.col("s"), r"(\w+)", 1).alias("first")
    ).collect()
    assert result[0]["first"] == "hello"
    print("PySpark: OK", result[0]["first"])
    spark.stop()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "robin"
    if mode == "robin":
        run_robin()
    else:
        run_pyspark()
