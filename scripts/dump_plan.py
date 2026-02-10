#!/usr/bin/env python3
"""
Pretty-print the logical plan for a minimal DataFrame pipeline (Phase 4 tooling).

Usage:
  python scripts/dump_plan.py

Creates a minimal session and DataFrame (filter + select), then prints Sparkless and
Robin logical plans. For custom pipelines, use from REPL or tests:

  from sparkless import SparkSession
  import sparkless.sql.functions as F
  from sparkless.utils.plan_debug import pretty_print_logical_plan
  spark = SparkSession("x")
  df = spark.createDataFrame([{"a": 1}, {"a": 2}]).filter(F.col("a") > 1).select("a")
  pretty_print_logical_plan(df, format="sparkless")
  pretty_print_logical_plan(df, format="robin")
"""

import sys
from pathlib import Path

# Ensure project root on path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from sparkless import SparkSession
import sparkless.sql.functions as F
from sparkless.utils.plan_debug import pretty_print_logical_plan

def main() -> None:
    spark = SparkSession("DumpPlanScript", backend_type="robin")
    df = spark.createDataFrame([{"a": 1, "b": 10}, {"a": 2, "b": 20}]).filter(F.col("a") > 1).select("a", "b")
    print("=== Sparkless logical plan ===")
    pretty_print_logical_plan(df, format="sparkless")
    print("\n=== Robin logical plan ===")
    try:
        pretty_print_logical_plan(df, format="robin")
    except ValueError as e:
        print(f"(Robin plan build failed: {e})")
    spark.stop()

if __name__ == "__main__":
    main()
