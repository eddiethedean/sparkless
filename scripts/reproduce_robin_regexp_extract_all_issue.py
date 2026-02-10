#!/usr/bin/env python3
"""
Reproduce the regexp_extract_all / select issue for robin-sparkless GitHub issue.

This script demonstrates:
1. PySpark-style usage: df.select(regexp_extract_all(col, pattern, idx).alias("m"))
2. Current robin-sparkless: select() only accepts column names (list of strings)
3. Impact: Sparkless Robin mode cannot run such tests (unsupported op or stall)

Run from repo root:
  python scripts/reproduce_robin_regexp_extract_all_issue.py

With robin-sparkless installed and Sparkless in Robin mode:
  SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python scripts/reproduce_robin_regexp_extract_all_issue.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure project root on path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def run_robin_direct() -> None:
    """Use robin_sparkless directly to show select/expression API gap."""
    section("1. robin-sparkless direct API")

    try:
        import robin_sparkless as rs
    except ImportError as e:
        print("robin_sparkless not installed. Skip with: pip install robin-sparkless")
        print(f"ImportError: {e}")
        return

    F = rs
    spark = (
        F.SparkSession.builder().app_name("repro-regexp-extract-all").get_or_create()
    )

    # Minimal data: one string column
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = [("s", "string")]

    print("Create DataFrame with create_dataframe_from_rows:")
    df = spark.create_dataframe_from_rows(data, schema)
    print(f"  Rows: {len(df.collect())}")

    print("\nSelect by column name (current supported usage):")
    try:
        out = df.select(["s"]).collect()
        print(f"  df.select(['s']).collect() -> {len(out)} rows: OK")
    except Exception as e:
        print(f"  FAILED: {type(e).__name__}: {e}")

    print("\nPySpark-style select with expression (regexp_extract_all):")
    print("  In PySpark: df.select(F.regexp_extract_all('s', r'\\d+', 0).alias('m'))")
    # Check if robin exposes regexp_extract_all or similar
    if hasattr(F, "regexp_extract_all"):
        try:
            out = df.select(
                [F.regexp_extract_all(F.col("s"), F.lit(r"\d+"), 0).alias("m")]
            ).collect()
            print(f"  robin_sparkless has regexp_extract_all -> {len(out)} rows")
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
    else:
        print(
            "  robin_sparkless has no F.regexp_extract_all (or similar) - cannot express this query."
        )
        print(
            "  select() also only accepts list of column names (strings), not Column expressions."
        )

    print(
        "\nConclusion: To run PySpark-style tests that use regexp_extract_all in select(),"
    )
    print("robin-sparkless would need either:")
    print(
        "  - select() to accept Column/expression arguments (not just string names), and"
    )
    print("  - a regexp_extract_all(column, pattern, group_index) function.")


def run_sparkless_robin_mode() -> None:
    """Run the equivalent test via Sparkless in Robin mode."""
    section("2. Sparkless in Robin mode (impact)")

    backend = (
        (
            os.environ.get("SPARKLESS_TEST_BACKEND")
            or os.environ.get("SPARKLESS_BACKEND")
            or ""
        )
        .strip()
        .lower()
    )
    if backend != "robin":
        print(
            "Not in Robin mode. Set SPARKLESS_TEST_BACKEND=robin and SPARKLESS_BACKEND=robin to reproduce."
        )
        print("When run in Robin mode, this test either:")
        print(
            "  - Raises SparkUnsupportedOperationError (Robin materializer does not support select with expressions),"
        )
        print("  - Or stalls (e.g. first-time Robin session init in a worker).")
        return

    from sparkless.sql import SparkSession
    from sparkless.sql import functions as F

    print("Creating SparkSession with backend=robin...")
    spark = SparkSession.builder.config(
        "spark.sparkless.backend", "robin"
    ).getOrCreate()
    print(
        "Creating DataFrame and calling select(regexp_extract_all(...)).collect() ..."
    )

    df = spark.createDataFrame(
        [
            {"s": "a1 b22 c333"},
            {"s": "no-digits"},
            {"s": None},
        ]
    )
    try:
        rows = df.select(F.regexp_extract_all("s", r"\d+", 0).alias("m")).collect()
        print(f"Result: {len(rows)} rows: {rows}")
    except Exception as e:
        print(f"Result: {type(e).__name__}: {e}")
    finally:
        import contextlib

        with contextlib.suppress(Exception):
            spark.stop()


def main() -> int:
    print("Reproduction script: robin-sparkless select + regexp_extract_all issue")
    run_robin_direct()
    run_sparkless_robin_mode()
    section("Done")
    print("Use the output above to fill the GitHub issue body.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
