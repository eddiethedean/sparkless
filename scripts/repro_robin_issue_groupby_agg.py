#!/usr/bin/env python3
"""
Reproduce: groupBy + agg (sum, count) logical plan parity.

Run:
  python scripts/repro_robin_issue_groupby_agg.py robin   # Robin-sparkless
  python scripts/repro_robin_issue_groupby_agg.py pyspark # PySpark expected
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> None:
    from sparkless import SparkSession
    from sparkless import functions as F

    spark = SparkSession.builder.appName("repro-groupby-agg").getOrCreate()
    data = [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}]
    df = spark.createDataFrame(data)
    result = df.groupBy("k").agg(
        F.sum("v").alias("sum(v)"),
        F.count("v").alias("count(v)"),
    )
    rows = result.collect()
    spark.stop()
    by_k = {r["k"]: r for r in rows}
    assert "a" in by_k and by_k["a"]["sum(v)"] == 30 and by_k["a"]["count(v)"] == 2
    assert "b" in by_k and by_k["b"]["sum(v)"] == 30 and by_k["b"]["count(v)"] == 1
    print("Robin: groupBy(k).agg(sum(v), count(v)) -> SUCCESS")
    print(f"  Rows: {rows}")


def run_pyspark() -> None:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql import functions as F

    spark = PySparkSession.builder.appName("repro-groupby-agg").getOrCreate()
    data = [{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "b", "v": 30}]
    df = spark.createDataFrame(data)
    result = df.groupBy("k").agg(
        F.sum("v").alias("sum(v)"),
        F.count("v").alias("count(v)"),
    )
    rows = result.collect()
    spark.stop()
    by_k = {r["k"]: r for r in rows}
    assert "a" in by_k and by_k["a"]["sum(v)"] == 30 and by_k["a"]["count(v)"] == 2
    assert "b" in by_k and by_k["b"]["sum(v)"] == 30 and by_k["b"]["count(v)"] == 1
    print("PySpark: groupBy(k).agg(sum(v), count(v)) -> SUCCESS (expected)")
    print(f"  Rows: {rows}")


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("robin", "pyspark"):
        print("Usage: python scripts/repro_robin_issue_groupby_agg.py robin|pyspark")
        return 1
    mode = sys.argv[1]
    try:
        if mode == "robin":
            run_robin()
        else:
            run_pyspark()
        return 0
    except Exception as e:
        print(f"{mode.upper()} FAILED: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
