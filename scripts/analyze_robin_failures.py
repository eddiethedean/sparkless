#!/usr/bin/env python3
"""
Parse test result file, group failures by category, exclude non-Robin.
Output: JSON and markdown summary for use by repro scripts and issue creation.

Usage:
  python scripts/analyze_robin_failures.py [--results path] [--output-dir dir] [--format json|md|both]

Default: reads test_results_full.txt from repo root, writes to scripts/robin_parity_repros/
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


# Patterns that indicate non-Robin (do not file as Robin issues)
EXCLUDE_PATTERNS = [
    r"ModuleNotFoundError: No module named 'polars'",
    r"DID NOT RAISE",
    r"test_notebooks\.py",
]

# Extract operation string from SparkUnsupportedOperationError / QueryExecutionException
OPS_RE = re.compile(r"Operation 'Operations: ([^']+)' is not supported")
QUERY_OPS_RE = re.compile(r"Operation 'Operations: ([^']+)' is not supported")


def normalize_failure_key(test_id: str, error_summary: str) -> tuple[str, str]:
    """
    Return (category_id, failure_key) for grouping.
    category_id: short slug for filenames (e.g. op_join, op_withColumn, col_not_found, parity_assertion)
    failure_key: human-readable key for the group (e.g. "Operations: join")
    """
    err = error_summary.strip()
    # Key A: SparkUnsupportedOperationError / QueryExecutionException
    m = OPS_RE.search(err) or QUERY_OPS_RE.search(err)
    if m:
        ops = m.group(1).strip()
        # Normalize to first op for category (join, withColumn, select, filter)
        if "join" in ops:
            return ("op_join", f"Operations: {ops}")
        if "withColumn" in ops or "with_column" in ops.lower():
            return ("op_withColumn", f"Operations: {ops}")
        if "select" in ops:
            return ("op_select", f"Operations: {ops}")
        if "filter" in ops:
            return ("op_filter", f"Operations: {ops}")
        return ("op_other", f"Operations: {ops}")

    # Key B: Column not found
    if "RuntimeError" in err and "not found" in err and "Column " in err and " not found" in err:
        return ("col_not_found", "RuntimeError: Column not found (alias/schema)")

    # Key C: AssertionError / parity
    if "AssertionError" in err or "DataFrames are not equivalent" in err or "Row count mismatch" in err:
        return ("parity_assertion", "AssertionError / DataFrames not equivalent")
    if err.startswith("assert ") or "assert " in err:
        return ("parity_assertion", "assert / wrong result")

    # Other
    return ("other", err[:80])


def is_excluded(test_id: str, error_summary: str) -> bool:
    for pat in EXCLUDE_PATTERNS:
        if re.search(pat, test_id + " " + error_summary):
            return True
    return False


def parse_result_file(path: Path) -> list[tuple[str, str]]:
    """Parse FAILED lines; return list of (test_id, error_summary)."""
    text = path.read_text(encoding="utf-8")
    failures: list[tuple[str, str]] = []
    for line in text.splitlines():
        line = line.rstrip()
        if not line.startswith("FAILED "):
            continue
        # FAILED tests/path::Test::test_name - error message
        match = re.match(r"^FAILED (tests/[^\s]+) - (.+)$", line)
        if match:
            test_id = match.group(1).strip()
            err = match.group(2).strip()
            failures.append((test_id, err))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Robin test failures and group by category")
    parser.add_argument(
        "--results",
        type=Path,
        default=None,
        help="Path to test result file (default: repo root test_results_full.txt)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for output (default: scripts/robin_parity_repros)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "md", "both"],
        default="both",
        help="Output format",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    results_path = args.results or root / "test_results_full.txt"
    out_dir = args.output_dir or root / "scripts" / "robin_parity_repros"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not results_path.exists():
        print(f"Results file not found: {results_path}", file=sys.stderr)
        return 1

    failures = parse_result_file(results_path)
    print(f"Parsed {len(failures)} FAILED lines from {results_path}")

    # Exclude non-Robin
    included: list[tuple[str, str]] = []
    excluded: list[tuple[str, str]] = []
    for test_id, err in failures:
        if is_excluded(test_id, err):
            excluded.append((test_id, err))
        else:
            included.append((test_id, err))

    print(f"Excluded {len(excluded)} (non-Robin); {len(included)} remaining")

    # Group by (category_id, failure_key)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for test_id, err in included:
        cat_id, fail_key = normalize_failure_key(test_id, err)
        key = (cat_id, fail_key)
        if key not in groups:
            groups[key] = {
                "category_id": cat_id,
                "failure_key": fail_key,
                "representative_test_ids": [],
                "example_error_message": err[:500],
                "count": 0,
            }
        grp = groups[key]
        grp["count"] += 1
        if len(grp["representative_test_ids"]) < 3:
            grp["representative_test_ids"].append(test_id)

    categories = sorted(groups.values(), key=lambda x: (-x["count"], x["category_id"]))

    # Output JSON
    if args.format in ("json", "both"):
        out_json = out_dir / "failure_categories.json"
        payload = {
            "source_file": str(results_path),
            "total_failures": len(failures),
            "excluded": len(excluded),
            "included": len(included),
            "categories": categories,
        }
        out_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"Wrote {out_json}")

    # Output Markdown
    if args.format in ("md", "both"):
        out_md = out_dir / "FAILURE_CATEGORIES.md"
        lines = [
            "# Robin test failure categories",
            "",
            f"Source: `{results_path}`",
            f"Total FAILED: {len(failures)} | Excluded (non-Robin): {len(excluded)} | Grouped: {len(included)}",
            "",
            "| Category ID | Failure key | Count | Representative tests |",
            "|-------------|-------------|-------|----------------------|",
        ]
        for c in categories:
            reps = ", ".join(c["representative_test_ids"][:2]) or "-"
            if len(reps) > 80:
                reps = reps[:77] + "..."
            lines.append(f"| {c['category_id']} | {c['failure_key'][:50]} | {c['count']} | {reps} |")
        lines.extend([
            "",
            "## Example errors",
            "",
        ])
        for c in categories[:20]:
            lines.append(f"### {c['category_id']} ({c['failure_key']})")
            lines.append("")
            lines.append("```")
            lines.append(c["example_error_message"][:400])
            lines.append("```")
            lines.append("")
        out_md.write_text("\n".join(lines), encoding="utf-8")
        print(f"Wrote {out_md}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
