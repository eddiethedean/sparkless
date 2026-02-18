#!/usr/bin/env python3
"""Reproduce robin-sparkless isin Int64 type mismatch vs PySpark parity.

Robin fails: 'is_in' cannot check for String values in Int64 data
PySpark: col("value").isin(1, 3) works when value is Int64.

Usage:
  python scripts/repro_robin_issue_isin_int64_type.py robin
  python scripts/repro_robin_issue_isin_int64_type.py pyspark
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
    df = spark.createDataFrame([{"value": 1}, {"value": 2}, {"value": 3}])
    try:
        result = df.filter(F.col("value").isin(1, 3)).collect()
        print("Robin: OK", len(result), [r["value"] for r in result])
    except Exception as e:
        print("Robin: FAIL", type(e).__name__, str(e))
    spark.stop()


def run_pyspark():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("repro").getOrCreate()
    df = spark.createDataFrame([{"value": 1}, {"value": 2}, {"value": 3}])
    result = df.filter(F.col("value").isin(1, 3)).collect()
    assert len(result) == 2 and {r["value"] for r in result} == {1, 3}
    print("PySpark: OK", len(result), [r["value"] for r in result])
    spark.stop()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "robin"
    if mode == "robin":
        run_robin()
    else:
        run_pyspark()
