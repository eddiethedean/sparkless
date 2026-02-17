#!/usr/bin/env python3
"""
Reproduce: row_number() over (partition by col) window parity.

Run:
  python scripts/repro_robin_issue_window_row_number.py robin
  python scripts/repro_robin_issue_window_row_number.py pyspark
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
    from sparkless.window import Window

    spark = SparkSession.builder.appName("repro-window-row-number").getOrCreate()
    data = [
        {"dept": "A", "salary": 10},
        {"dept": "A", "salary": 20},
        {"dept": "B", "salary": 30},
    ]
    df = spark.createDataFrame(data)
    win = Window.partitionBy("dept")
    result = df.withColumn("rn", F.row_number().over(win)).select("dept", "salary", "rn")
    rows = result.collect()
    spark.stop()
    by_dept = {}
    for r in rows:
        d = r["dept"]
        if d not in by_dept:
            by_dept[d] = []
        by_dept[d].append(r["rn"])
    assert by_dept["A"] == [1, 2] and by_dept["B"] == [1]
    print("Robin: row_number() over (partition by dept) -> SUCCESS")
    print(f"  rn by dept: {by_dept}")


def run_pyspark() -> None:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = PySparkSession.builder.appName("repro-window-row-number").getOrCreate()
    data = [
        {"dept": "A", "salary": 10},
        {"dept": "A", "salary": 20},
        {"dept": "B", "salary": 30},
    ]
    df = spark.createDataFrame(data)
    win = Window.partitionBy("dept")
    result = df.withColumn("rn", F.row_number().over(win)).select("dept", "salary", "rn")
    rows = result.collect()
    spark.stop()
    by_dept = {}
    for r in rows:
        d = r["dept"]
        if d not in by_dept:
            by_dept[d] = []
        by_dept[d].append(r["rn"])
    assert by_dept["A"] == [1, 2] and by_dept["B"] == [1]
    print("PySpark: row_number() over (partition by dept) -> SUCCESS (expected)")
    print(f"  rn by dept: {by_dept}")


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("robin", "pyspark"):
        print("Usage: python scripts/repro_robin_issue_window_row_number.py robin|pyspark")
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
