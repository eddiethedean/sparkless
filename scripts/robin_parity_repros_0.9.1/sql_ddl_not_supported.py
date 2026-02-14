#!/usr/bin/env python3
"""
Repro: SQL DDL (CREATE SCHEMA, CREATE DATABASE) not supported.
Robin 0.9.1: SQL: only SELECT is supported, got CreateSchema / CreateDatabase.

Run from repo root: python scripts/robin_parity_repros_0.9.1/sql_ddl_not_supported.py
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
    spark = rs.SparkSession.builder().app_name("repro-sql-ddl").get_or_create()
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        ROBIN_OK.append("CREATE SCHEMA succeeded")
    except Exception as e:
        ROBIN_FAIL.append(f"CREATE SCHEMA: {type(e).__name__}: {e}")
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS show_db")
        ROBIN_OK.append("CREATE DATABASE succeeded")
    except Exception as e:
        ROBIN_FAIL.append(f"CREATE DATABASE: {type(e).__name__}: {e}")


def run_pyspark() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        PYSPARK_SKIP.append("pyspark not installed")
        return
    spark = SparkSession.builder.master("local[1]").appName("repro").getOrCreate()
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        PYSPARK_OK.append("CREATE SCHEMA succeeded")
        spark.sql("CREATE DATABASE IF NOT EXISTS show_db")
        PYSPARK_OK.append("CREATE DATABASE succeeded")
    except Exception as e:
        PYSPARK_OK.append(f"pyspark: {e}")
    finally:
        spark.stop()


def main() -> int:
    print("sql_ddl_not_supported: CREATE SCHEMA / CREATE DATABASE")
    run_robin()
    run_pyspark()
    print("Robin:", "OK" if ROBIN_OK and not ROBIN_FAIL else "FAILED", ROBIN_OK or ROBIN_FAIL)
    print("PySpark:", "OK" if PYSPARK_OK else "SKIP", PYSPARK_OK or PYSPARK_SKIP)
    return 0 if ROBIN_FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
