#!/usr/bin/env python3
"""
Benchmark Sparkless (Robin) vs PySpark: session creation, simple query, window function.

Run from repo root:
  python scripts/benchmark_sparkless_vs_pyspark.py

Prints mean times (seconds) and speedup. PySpark is skipped if not installed.
"""

from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WARMUP = 1
RUNS = 5
ROWS = 10_000


def _time_operation(operation, name: str) -> float | None:
    """Run operation WARMUP+RUNS times, return mean of last RUNS. Returns None on error."""
    times = []
    for i in range(WARMUP + RUNS):
        start = time.perf_counter()
        try:
            operation()
        except Exception as e:
            print(f"{name} failed: {e}", file=sys.stderr)
            return None
        elapsed = time.perf_counter() - start
        if i >= WARMUP:
            times.append(elapsed)
    return statistics.mean(times)


def run_sparkless_benchmarks() -> dict[str, float]:
    from sparkless import SparkSession

    def session_create():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        spark.stop()

    def simple_query():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        data = [{"id": j, "x": j * 2} for j in range(ROWS)]
        df = spark.createDataFrame(data)
        df.select("id", "x").limit(1000).collect()
        spark.stop()

    def count_op():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        data = [{"id": j, "x": j * 2} for j in range(ROWS)]
        df = spark.createDataFrame(data)
        df.count()
        spark.stop()

    return {
        "session_create": _time_operation(session_create, "Sparkless session"),
        "simple_query": _time_operation(simple_query, "Sparkless simple query"),
        "count": _time_operation(count_op, "Sparkless count"),
    }


def run_pyspark_benchmarks() -> dict[str, float] | None:
    try:
        from pyspark.sql import SparkSession as PySparkSession
    except ImportError:
        print("PySpark not installed; skipping PySpark benchmarks.", file=sys.stderr)
        return None

    def session_create():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        spark.stop()

    def simple_query():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        data = [{"id": j, "x": j * 2} for j in range(ROWS)]
        df = spark.createDataFrame(data)
        df.select("id", "x").limit(1000).collect()
        spark.stop()

    def count_op():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        data = [{"id": j, "x": j * 2} for j in range(ROWS)]
        df = spark.createDataFrame(data)
        df.count()
        spark.stop()

    return {
        "session_create": _time_operation(session_create, "PySpark session"),
        "simple_query": _time_operation(simple_query, "PySpark simple query"),
        "count": _time_operation(count_op, "PySpark count"),
    }


def main() -> int:
    print("Benchmark Sparkless (Robin) vs PySpark")
    print(f"  Rows: {ROWS}, warmup: {WARMUP}, runs: {RUNS}\n")

    print("Running Sparkless (Robin) benchmarks...")
    sparkless = run_sparkless_benchmarks()
    print("Running PySpark benchmarks...")
    pyspark = run_pyspark_benchmarks()

    print("\nResults (mean seconds)")
    print("-" * 65)
    print(f"{'Operation':<25} {'Sparkless':<14} {'PySpark':<14} {'Speedup':<10}")
    print("-" * 65)

    for key, label in [
        ("session_create", "Session creation"),
        ("simple_query", "Simple query (select+limit+collect)"),
        ("count", "Count (createDataFrame+count)"),
    ]:
        s = sparkless.get(key)
        p = pyspark.get(key) if pyspark else None
        if s is None:
            print(f"{label:<25} {'ERROR':<14} {str(p) if p is not None else 'N/A':<14}")
            continue
        s_str = f"{s:.3f}"
        p_str = f"{p:.3f}" if p is not None else "N/A"
        if p is not None and p > 0:
            speedup = p / s
            speedup_str = f"{speedup:.0f}x"
        else:
            speedup_str = "-"
        print(f"{label:<25} {s_str:<14} {p_str:<14} {speedup_str:<10}")

    # Summary for README
    print("\n--- Summary for README ---")
    if sparkless.get("session_create") is not None:
        sc = sparkless["session_create"]
        pc = pyspark.get("session_create") if pyspark else None
        if pc and pc > 0:
            print(f"Session Creation: Sparkless={sc:.2f}s, PySpark={pc:.1f}s, Speedup={pc/sc:.0f}x")
        else:
            print(f"Session Creation: Sparkless={sc:.2f}s (PySpark typical 30-45s)")
    if sparkless.get("simple_query") is not None:
        sq = sparkless["simple_query"]
        pq = pyspark.get("simple_query") if pyspark else None
        if pq and pq > 0:
            print(f"Simple Query: Sparkless={sq:.3f}s, PySpark={pq:.2f}s, Speedup={pq/sq:.0f}x")
        else:
            print(f"Simple Query: Sparkless={sq:.3f}s")
    if sparkless.get("count") is not None:
        sk = sparkless["count"]
        pk = pyspark.get("count") if pyspark else None
        if pk and pk > 0:
            print(f"Count: Sparkless={sk:.3f}s, PySpark={pk:.2f}s, Speedup={pk/sk:.0f}x")
        else:
            print(f"Count: Sparkless={sk:.3f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
