#!/usr/bin/env python3
"""
Reproduce: filter(between) + withColumn(power) + withColumn(cast) parity.

Run:
  python scripts/repro_robin_issue_between_power_cast.py robin
  python scripts/repro_robin_issue_between_power_cast.py pyspark
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

    spark = SparkSession.builder.appName("repro-between-power-cast").getOrCreate()
    data = [{"a": 2, "b": 10}, {"a": 5, "b": 20}, {"a": 8, "b": 30}]
    df = spark.createDataFrame(data)
    # filter: a between 3 and 7 -> one row a=5; then squared = a**2, a_str = cast(a, string)
    result = (
        df.filter(F.col("a").between(3, 7))
        .withColumn("squared", F.col("a") ** 2)
        .withColumn("a_str", F.col("a").cast("string"))
    )
    rows = result.collect()
    spark.stop()
    assert len(rows) == 1
    assert rows[0]["a"] == 5 and rows[0]["squared"] == 25 and rows[0]["a_str"] == "5"
    print("Robin: filter(between) + withColumn(**2) + withColumn(cast) -> SUCCESS")
    print(f"  Row: {dict(rows[0])}")


def run_pyspark() -> None:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql import functions as F

    spark = PySparkSession.builder.appName("repro-between-power-cast").getOrCreate()
    data = [{"a": 2, "b": 10}, {"a": 5, "b": 20}, {"a": 8, "b": 30}]
    df = spark.createDataFrame(data)
    result = (
        df.filter(F.col("a").between(3, 7))
        .withColumn("squared", F.col("a") ** 2)
        .withColumn("a_str", F.col("a").cast("string"))
    )
    rows = result.collect()
    spark.stop()
    assert len(rows) == 1
    r = rows[0]
    assert r["a"] == 5 and r["squared"] == 25 and r["a_str"] == "5"
    print("PySpark: filter(between) + withColumn(**2) + withColumn(cast) -> SUCCESS (expected)")
    print(f"  Row: {dict(r)}")


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("robin", "pyspark"):
        print("Usage: python scripts/repro_robin_issue_between_power_cast.py robin|pyspark")
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
