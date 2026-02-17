#!/usr/bin/env python3
"""
Benchmark Sparkless (Robin) vs PySpark: session creation, simple/complex queries.

Run from repo root with the venv that has the wheel built with robin-sparkless 0.11.3:
  .venv/bin/python scripts/benchmark_sparkless_vs_pyspark.py

To ensure robin-sparkless 0.11.3: maturin build --release && .venv/bin/pip install target/wheels/sparkless-*.whl --force-reinstall

Prints mean times (seconds) and speedup. PySpark is skipped if not installed.
Dataset: 100k rows (id, x, key, value). Filter/withColumn may show ERROR on some
Robin versions due to expression plan format; other queries (select, count, groupBy+agg,
orderBy+limit) run on Robin 0.11.3+.
"""

from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Use installed wheel when present (ensures robin-sparkless 0.11.3). Only add repo to path if not installed.
try:
    import sparkless  # noqa: F401
except ImportError:
    sys.path.insert(0, str(ROOT))

WARMUP = 1
RUNS = 5
ROWS = 100_000


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


def _bench_data():
    """Build 100k-row dataset: id, x, key (for groupBy), value."""
    return [
        {"id": j, "x": j * 2, "key": j % 100, "value": j * 2}
        for j in range(ROWS)
    ]


def run_sparkless_benchmarks() -> dict[str, float]:
    from sparkless import SparkSession
    from sparkless import functions as F

    def session_create():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        spark.stop()

    def simple_query():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.select("id", "x").limit(1000).collect()
        spark.stop()

    def count_op():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.count()
        spark.stop()

    def filter_select():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.filter(F.col("x") > 50_000).select("id", "x", "key").limit(1000).collect()
        spark.stop()

    def with_column():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.withColumn("double_x", F.col("x") * 2).select("id", "x", "double_x").limit(1000).collect()
        spark.stop()

    def group_by_agg():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.groupBy("key").agg(
            F.sum("value").alias("sum_val"),
            F.count("*").alias("cnt"),
        ).collect()
        spark.stop()

    def order_by_limit():
        spark = SparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.orderBy(F.desc("id")).limit(1000).collect()
        spark.stop()

    return {
        "session_create": _time_operation(session_create, "Sparkless session"),
        "simple_query": _time_operation(simple_query, "Sparkless simple query"),
        "count": _time_operation(count_op, "Sparkless count"),
        "filter_select": _time_operation(filter_select, "Sparkless filter+select"),
        "with_column": _time_operation(with_column, "Sparkless withColumn"),
        "group_by_agg": _time_operation(group_by_agg, "Sparkless groupBy+agg"),
        "order_by_limit": _time_operation(order_by_limit, "Sparkless orderBy+limit"),
    }


def run_pyspark_benchmarks() -> dict[str, float] | None:
    try:
        from pyspark.sql import SparkSession as PySparkSession
        from pyspark.sql import functions as F
    except ImportError:
        print("PySpark not installed; skipping PySpark benchmarks.", file=sys.stderr)
        return None

    def session_create():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        spark.stop()

    def simple_query():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.select("id", "x").limit(1000).collect()
        spark.stop()

    def count_op():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.count()
        spark.stop()

    def filter_select():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.filter(F.col("x") > 50_000).select("id", "x", "key").limit(1000).collect()
        spark.stop()

    def with_column():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.withColumn("double_x", F.col("x") * 2).select("id", "x", "double_x").limit(1000).collect()
        spark.stop()

    def group_by_agg():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.groupBy("key").agg(
            F.sum("value").alias("sum_val"),
            F.count("*").alias("cnt"),
        ).collect()
        spark.stop()

    def order_by_limit():
        spark = PySparkSession.builder.appName("bench").getOrCreate()
        df = spark.createDataFrame(_bench_data())
        df.orderBy(F.desc("id")).limit(1000).collect()
        spark.stop()

    return {
        "session_create": _time_operation(session_create, "PySpark session"),
        "simple_query": _time_operation(simple_query, "PySpark simple query"),
        "count": _time_operation(count_op, "PySpark count"),
        "filter_select": _time_operation(filter_select, "PySpark filter+select"),
        "with_column": _time_operation(with_column, "PySpark withColumn"),
        "group_by_agg": _time_operation(group_by_agg, "PySpark groupBy+agg"),
        "order_by_limit": _time_operation(order_by_limit, "PySpark orderBy+limit"),
    }


def _check_robin_select_compat() -> None:
    """Fail fast if select-by-name fails (e.g. extension built with robin-sparkless < 0.11.3)."""
    from sparkless import SparkSession

    spark = SparkSession.builder.appName("bench-check").getOrCreate()
    try:
        df = spark.createDataFrame([{"id": 1, "x": 2}])
        df.select("id", "x").collect()
    except Exception as e:
        err = str(e)
        if "select payload must be array" in err or "invalid plan" in err.lower():
            print(
                "ERROR: Sparkless select-by-name failed. The extension may not be built with\n"
                "robin-sparkless 0.11.3. Run: maturin build --release &&\n"
                "  .venv/bin/pip install target/wheels/sparkless-*.whl --force-reinstall\n"
                "Then run this script with: .venv/bin/python scripts/benchmark_sparkless_vs_pyspark.py",
                file=sys.stderr,
            )
            sys.exit(1)
        raise
    finally:
        spark.stop()


def main() -> int:
    print("Benchmark Sparkless (Robin) vs PySpark")
    print(f"  Rows: {ROWS}, warmup: {WARMUP}, runs: {RUNS}\n")

    print("Checking Sparkless select (robin-sparkless 0.11.3 compat)...")
    _check_robin_select_compat()
    print("OK\n")

    print("Running Sparkless (Robin) benchmarks...")
    sparkless = run_sparkless_benchmarks()
    print("Running PySpark benchmarks...")
    pyspark = run_pyspark_benchmarks()

    ops = [
        ("session_create", "Session creation"),
        ("simple_query", "Simple query (select+limit)"),
        ("count", "Count"),
        ("filter_select", "Filter + select + limit"),
        ("with_column", "WithColumn + select + limit"),
        ("group_by_agg", "GroupBy + sum/count agg"),
        ("order_by_limit", "OrderBy desc + limit"),
    ]
    col_w = 32
    print("\nResults (mean seconds)")
    print("-" * (col_w + 14 + 14 + 10))
    print(f"{'Operation':<{col_w}} {'Sparkless':<14} {'PySpark':<14} {'Speedup':<10}")
    print("-" * (col_w + 14 + 14 + 10))

    for key, label in ops:
        s = sparkless.get(key)
        p = pyspark.get(key) if pyspark else None
        if s is None:
            print(f"{label:<{col_w}} {'ERROR':<14} {str(p) if p is not None else 'N/A':<14}")
            continue
        s_str = f"{s:.3f}"
        p_str = f"{p:.3f}" if p is not None else "N/A"
        if p is not None and p > 0:
            speedup = p / s
            speedup_str = f"{speedup:.0f}x"
        else:
            speedup_str = "-"
        print(f"{label:<{col_w}} {s_str:<14} {p_str:<14} {speedup_str:<10}")

    # Summary for README
    print("\n--- Summary for README ---")
    for key, name in ops:
        sk = sparkless.get(key)
        pk = pyspark.get(key) if pyspark else None
        if sk is None:
            continue
        if pk is not None and pk > 0:
            print(f"{name}: Sparkless={sk:.3f}s, PySpark={pk:.3f}s, Speedup={pk/sk:.0f}x")
        else:
            print(f"{name}: Sparkless={sk:.3f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
