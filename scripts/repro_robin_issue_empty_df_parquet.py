#!/usr/bin/env python3
"""
Reproduce: Empty DataFrame with explicit schema + parquet table append (PySpark parity).

Run:
  # Robin-sparkless (Sparkless v4 Robin mode; may raise or fail)
  python scripts/repro_robin_issue_empty_df_parquet.py robin

  # PySpark (expected behavior)
  python scripts/repro_robin_issue_empty_df_parquet.py pyspark
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def run_robin() -> None:
    """Sparkless v4 Robin mode: createDataFrame([], schema), parquet table, append."""
    from sparkless import SparkSession
    from sparkless.spark_types import StructType, StructField, IntegerType, StringType

    spark = SparkSession.builder.appName("repro-empty-df-parquet").getOrCreate()
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )

    # 1) Empty DataFrame with explicit schema (PySpark allows this)
    empty_df = spark.createDataFrame([], schema)
    print("Robin: createDataFrame([], schema) -> OK")

    # 2) Create table from empty DF, then append and read back
    fqn = "test_schema.repro_empty_table"
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    try:
        empty_df.write.format("parquet").mode("overwrite").saveAsTable(fqn)
        print("Robin: empty_df.write.saveAsTable(...) -> OK")
        r1 = spark.table(fqn)
        print(f"Robin: spark.table() count = {r1.count()} (expected 0)")

        df1 = spark.createDataFrame([{"id": 1, "name": "a"}], schema)
        df1.write.format("parquet").mode("append").saveAsTable(fqn)
        r2 = spark.table(fqn)
        print(f"Robin: after append count = {r2.count()} (expected 1)")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        spark.sql("DROP SCHEMA IF EXISTS test_schema CASCADE")
    print("Robin: SUCCESS (no error)")
    spark.stop()


def run_pyspark() -> None:
    """PySpark: same flow, expected behavior."""
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    spark = PySparkSession.builder.appName("repro-empty-df-parquet").getOrCreate()
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )

    empty_df = spark.createDataFrame([], schema)
    print("PySpark: createDataFrame([], schema) -> OK")

    fqn = "test_schema.repro_empty_table"
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    try:
        empty_df.write.format("parquet").mode("overwrite").saveAsTable(fqn)
        print("PySpark: empty_df.write.saveAsTable(...) -> OK")
        r1 = spark.table(fqn)
        print(f"PySpark: spark.table() count = {r1.count()} (expected 0)")

        df1 = spark.createDataFrame([{"id": 1, "name": "a"}], schema)
        df1.write.format("parquet").mode("append").saveAsTable(fqn)
        r2 = spark.table(fqn)
        print(f"PySpark: after append count = {r2.count()} (expected 1)")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        spark.sql("DROP SCHEMA IF EXISTS test_schema CASCADE")
    print("PySpark: SUCCESS (expected behavior)")
    spark.stop()


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("robin", "pyspark"):
        print(
            "Usage: python scripts/repro_robin_issue_empty_df_parquet.py robin|pyspark"
        )
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
