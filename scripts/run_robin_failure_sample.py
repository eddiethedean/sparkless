#!/usr/bin/env python3
"""
Run a fixed list of Robin-mode failing tests with full tracebacks and append
output to a file for parsing (exception types and messages).

Usage:
  SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python scripts/run_robin_failure_sample.py [--output FILE] [--max N]

Reads failing test IDs from tests/robin_mode_test_results.txt (or a provided list),
runs up to --max tests (default 50) with --tb=long, and appends to --output
(default tests/robin_failures_sample.txt).
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def extract_failed_test_ids(results_path: Path, limit: int) -> list[str]:
    """Parse FAILED lines from a pytest results file; return up to limit unique IDs."""
    seen: set[str] = set()
    ids: list[str] = []
    pattern = re.compile(r"FAILED\s+(tests/[^\s]+?)(?:\s+<-|$)")
    with open(results_path) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                tid = m.group(1).strip()
                if tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
                    if len(ids) >= limit:
                        break
    return ids


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "tests" / "robin_failures_sample.txt",
        help="Append output to this file",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=50,
        help="Max number of failing tests to run (default 50)",
    )
    parser.add_argument(
        "--results",
        type=Path,
        default=root / "tests" / "robin_mode_test_results.txt",
        help="Path to robin mode results file to extract FAILED IDs",
    )
    parser.add_argument(
        "--parquet-only",
        action="store_true",
        help="Only run the 7 parquet ERROR tests",
    )
    args = parser.parse_args()

    if args.parquet_only:
        test_ids = [
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_parquet_format_append_to_existing_table",
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_parquet_format_append_to_new_table",
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_parquet_format_multiple_append_operations",
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_parquet_format_append_detached_df_visible_to_active_session",
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_parquet_format_append_detached_df_visible_to_multiple_sessions",
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_storage_manager_detached_write_visible_to_session",
            "tests/parity/dataframe/test_parquet_format_table_append.py::TestParquetFormatTableAppend::test_pipeline_logs_like_write_visible_immediately",
        ]
    elif args.results.exists():
        test_ids = extract_failed_test_ids(args.results, args.max)
    else:
        print(f"Results file not found: {args.results}", file=sys.stderr)
        return 1

    env = {
        **__import__("os").environ,
        "SPARKLESS_TEST_BACKEND": "robin",
        "SPARKLESS_BACKEND": "robin",
    }
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        *test_ids,
        "-v",
        "--tb=long",
        "-p", "no:cacheprovider",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "a") as out:
        out.write("\n\n=== run_robin_failure_sample.py ===\n")
        out.write(" ".join(cmd) + "\n\n")
    proc = subprocess.run(
        cmd,
        cwd=root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=600,
    )
    with open(args.output, "a") as out:
        out.write(proc.stdout)
    print(f"Appended output to {args.output}")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
