#!/usr/bin/env python3
"""
Reproduce confirmed robin-sparkless vs PySpark parity issues.

Run:
  python scripts/repro_robin_parity_issues.py robin   # Run Robin (Sparkless) side
  python scripts/repro_robin_parity_issues.py pyspark # Run PySpark (expected) side

Confirmed issues (reported to eddiethedean/robin-sparkless):
  1. create_map() empty: Robin returns None, PySpark returns {}
     https://github.com/eddiethedean/robin-sparkless/issues/578
  2. first() after orderBy: Robin ignores sort, returns first in storage order
     https://github.com/eddiethedean/robin-sparkless/issues/579
  3. Join with column expr when both have same col: Robin fails "duplicate column"
     https://github.com/eddiethedean/robin-sparkless/issues/580
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> None:
    """Run Robin (Sparkless) repros."""
    from sparkless.sql import SparkSession
    from sparkless.sql import functions as F

    spark = SparkSession.builder.appName("repro-parity").getOrCreate()

    # 1. create_map empty
    df = spark.createDataFrame([(1,)], ["id"])
    result = df.withColumn("m", F.create_map()).collect()
    m_val = result[0]["m"]
    status = "PASS" if m_val == {} else "FAIL"
    print(f"1. create_map() empty: {status} (got {m_val!r}, expected {{}})")

    # 2. first() after orderBy
    data = [
        {"name": "Charlie", "value": 3},
        {"name": "Alice", "value": 1},
        {"name": "Bob", "value": 2},
    ]
    df = spark.createDataFrame(data)
    first_name = df.orderBy("value").first()["name"] if df.orderBy("value").first() else None
    status = "PASS" if first_name == "Alice" else "FAIL"
    print(f"2. first() after orderBy(value): {status} (got {first_name!r}, expected Alice)")

    # 3. Join with column expr (duplicate col names)
    emp = spark.createDataFrame(
        [
            {"id": 1, "name": "Alice", "dept_id": 10},
            {"id": 2, "name": "Bob", "dept_id": 20},
            {"id": 3, "name": "Charlie", "dept_id": 10},
        ]
    )
    dept = spark.createDataFrame(
        [
            {"dept_id": 10, "name": "IT"},
            {"dept_id": 20, "name": "HR"},
        ]
    )
    try:
        c = emp.join(dept, emp.dept_id == dept.dept_id, "inner").count()
        status = "PASS" if c == 3 else "FAIL"
        print(f"3. join(col expr, duplicate col): {status} (got count={c}, expected 3)")
    except Exception as e:
        print(f"3. join(col expr, duplicate col): FAIL ({type(e).__name__}: {str(e)[:80]}...)")

    spark.stop()


def run_pyspark() -> None:
    """Run PySpark (expected) repros."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("repro-parity").getOrCreate()

    # 1. create_map empty
    df = spark.createDataFrame([(1,)], ["id"])
    result = df.withColumn("m", F.create_map()).collect()
    m_val = result[0]["m"]
    assert m_val == {}, f"Expected {{}}, got {m_val!r}"
    print("1. create_map() empty: PASS (expected)")

    # 2. first() after orderBy
    data = [
        {"name": "Charlie", "value": 3},
        {"name": "Alice", "value": 1},
        {"name": "Bob", "value": 2},
    ]
    df = spark.createDataFrame(data)
    first_name = df.orderBy("value").first()["name"]
    assert first_name == "Alice", f"Expected Alice, got {first_name!r}"
    print("2. first() after orderBy(value): PASS (expected)")

    # 3. Join with column expr
    emp = spark.createDataFrame(
        [
            {"id": 1, "name": "Alice", "dept_id": 10},
            {"id": 2, "name": "Bob", "dept_id": 20},
            {"id": 3, "name": "Charlie", "dept_id": 10},
        ]
    )
    dept = spark.createDataFrame(
        [
            {"dept_id": 10, "name": "IT"},
            {"dept_id": 20, "name": "HR"},
        ]
    )
    c = emp.join(dept, emp.dept_id == dept.dept_id, "inner").count()
    assert c == 3, f"Expected 3, got {c}"
    print("3. join(col expr, duplicate col): PASS (expected)")

    spark.stop()


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("robin", "pyspark"):
        print("Usage: python scripts/repro_robin_parity_issues.py robin|pyspark")
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
