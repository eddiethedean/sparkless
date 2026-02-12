#!/usr/bin/env python3
"""Compare the lightweight pandas stub with the optional native pandas backend."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import statistics
import sys
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


def _load_pandas_backend(mode: str):
    """Import the pandas module using the requested backend mode."""

    os.environ["MOCK_SPARK_PANDAS_MODE"] = mode
    if "pandas" in sys.modules:
        del sys.modules["pandas"]
    return importlib.import_module("pandas")


def _time_function(func: Callable[[], Any], samples: int) -> list[float]:
    timings: list[float] = []
    for _ in range(samples):
        start = time.perf_counter()
        func()
        timings.append((time.perf_counter() - start) * 1000.0)
    return timings


def _summarise(samples: list[float]) -> dict[str, float]:
    return {
        "mean_ms": statistics.mean(samples),
        "p50_ms": statistics.median(samples),
        "p95_ms": statistics.quantiles(samples, n=100)[94],
        "min_ms": min(samples),
        "max_ms": max(samples),
    }


def benchmark_backend(mode: str, rows: int, samples: int) -> dict[str, Any]:
    pandas_module = _load_pandas_backend(mode)
    backend_label = getattr(pandas_module, "get_backend", lambda: mode)()

    dataset = [
        {"id": i, "category": f"c{i % 5}", "value": float(i), "flag": i % 2 == 0}
        for i in range(rows)
    ]

    creation_timings = _time_function(
        lambda: pandas_module.DataFrame(dataset), samples=samples
    )

    frame = pandas_module.DataFrame(dataset)

    to_dict_timings = _time_function(
        lambda: frame.to_dict(orient="records"), samples=samples
    )

    partition_size = max(rows // 10, 1)
    partitions = [
        pandas_module.DataFrame(dataset[i : i + partition_size])
        for i in range(0, rows, partition_size)
    ]

    concat_timings = _time_function(
        lambda: pandas_module.concat(partitions, ignore_index=True),
        samples=samples,
    )

    iloc_timings = _time_function(lambda: frame.iloc[:partition_size], samples=samples)

    return {
        "mode": backend_label,
        "create": _summarise(creation_timings),
        "to_dict": _summarise(to_dict_timings),
        "concat": _summarise(concat_timings),
        "iloc": _summarise(iloc_timings),
    }


def print_report(results: list[dict[str, dict[str, float]]]) -> None:
    headers = [
        "Backend",
        "Create (mean ms)",
        "to_dict (mean ms)",
        "concat (mean ms)",
        "iloc (mean ms)",
    ]
    print("\n".join([" | ".join(headers), "-" * 72]))
    for entry in results:
        print(
            f"{entry['mode']:<10} | "
            f"{entry['create']['mean_ms']:<17.3f} | "
            f"{entry['to_dict']['mean_ms']:<17.3f} | "
            f"{entry['concat']['mean_ms']:<16.3f} | "
            f"{entry['iloc']['mean_ms']:<13.3f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark the Mock Spark pandas shim against native pandas."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Number of synthetic rows to generate (default: %(default)s).",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Number of timing samples per scenario (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional path to write benchmark results as JSON.",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        default=["stub", "native"],
        help="Backends to benchmark (subset of: stub, native, auto).",
    )

    args = parser.parse_args()
    results: list[dict[str, Any]] = []

    for mode in args.modes:
        try:
            result = benchmark_backend(mode, rows=args.rows, samples=args.samples)
        except ModuleNotFoundError as exc:
            print(f"[skip] {mode}: {exc}")
            continue
        results.append(result)

    if not results:
        print("No benchmarks executed; ensure requested backends are available.")
        return

    print_report(results)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2, default=str)
        print(f"\nWrote detailed metrics to {args.output}")


if __name__ == "__main__":
    main()
