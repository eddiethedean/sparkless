#!/usr/bin/env python3
"""
Create GitHub issues in eddiethedean/robin-sparkless for Robin–PySpark parity gaps:
tests that FAIL with Robin backend but PASS with PySpark. Uses result files from
  - tests/robin_parity_broad_results.txt (Robin run)
  - tests/pyspark_parity_failed_results.txt (PySpark run on same tests)
and excludes the 17 issues already filed via create_robin_github_issues.py.

Run from repo root:
  python scripts/create_robin_github_issues_from_results.py [--dry-run]

  # Use custom result files (e.g. for parity/sql + parity/internal):
  python scripts/create_robin_github_issues_from_results.py \\
    --robin-results tests/robin_parity_sql_internal_results.txt \\
    --pyspark-results tests/pyspark_parity_sql_internal_results.txt \\
    [--no-already-filed]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"

# Full test paths for the 17 issues already created (issues #1–#17).
ALREADY_FILED = {
    "tests/parity/dataframe/test_join.py::TestJoinParity::test_inner_join",
    "tests/parity/dataframe/test_join.py::TestJoinParity::test_left_join",
    "tests/parity/dataframe/test_join.py::TestJoinParity::test_right_join",
    "tests/parity/dataframe/test_join.py::TestJoinParity::test_outer_join",
    "tests/parity/dataframe/test_join.py::TestJoinParity::test_semi_join",
    "tests/parity/dataframe/test_join.py::TestJoinParity::test_anti_join",
    "tests/parity/dataframe/test_filter.py::TestFilterParity::test_filter_operations",
    "tests/parity/dataframe/test_filter.py::TestFilterParity::test_filter_with_boolean",
    "tests/parity/dataframe/test_filter.py::TestFilterParity::test_filter_with_and_operator",
    "tests/parity/dataframe/test_filter.py::TestFilterParity::test_filter_with_or_operator",
    "tests/parity/dataframe/test_filter.py::TestFilterParity::test_filter_on_table_with_complex_schema",
    "tests/parity/dataframe/test_select.py::TestSelectParity::test_select_with_alias",
    "tests/parity/dataframe/test_select.py::TestSelectParity::test_column_access",
    "tests/parity/dataframe/test_transformations.py::TestTransformationsParity::test_with_column",
    "tests/parity/dataframe/test_transformations.py::TestTransformationsParity::test_drop_column",
    "tests/parity/dataframe/test_transformations.py::TestTransformationsParity::test_distinct",
    "tests/parity/dataframe/test_transformations.py::TestTransformationsParity::test_order_by_desc",
}


def parse_robin_failures(robin_results_path: Path) -> dict[str, str]:
    """Parse FAILED lines from Robin run; map test_id -> first line of error."""
    text = robin_results_path.read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for m in re.finditer(r"^FAILED (tests/parity/[^\s]+) - (.+)$", text, re.MULTILINE):
        test_id, err_first = m.group(1).strip(), m.group(2).strip()
        # Optionally add next line if it's a short continuation (e.g. Row count mismatch)
        rest = text[m.end() : m.end() + 200]
        next_line = rest.split("\n")[0].strip() if rest else ""
        if next_line.startswith("Row count mismatch") or next_line.startswith(
            "assert "
        ):
            err_first = err_first + "\n" + next_line
        out[test_id] = err_first[:500]  # cap length
    return out


def parse_pyspark_passed(pyspark_results_path: Path) -> set[str]:
    """Collect test IDs that show PASSED in PySpark run."""
    text = pyspark_results_path.read_text(encoding="utf-8")
    passed = set()
    for m in re.finditer(r"^(tests/parity/[^\s]+) PASSED ", text, re.MULTILINE):
        passed.add(m.group(1).strip())
    return passed


def make_body(test_path: str, error: str) -> str:
    repro_path = test_path
    return f"""## Summary

When [Sparkless](https://github.com/eddiethedean/sparkless) runs its parity test suite with the **Robin backend** (robin-sparkless), this test fails. The same test **passes** with the PySpark backend.

## Test

- **Suite:** Sparkless parity tests
- **Test:** `{test_path}`

## Error (Robin backend)

```
{error}
```

## Expected

Test should produce the same result as with PySpark (parity).

## Actual

See error above (wrong row count, exception, or unsupported operation).

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo (with `robin-sparkless` installed):

```bash
pip install robin-sparkless
cd /path/to/sparkless
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest \\
  {repro_path} \\
  -v --tb=short
```

## Context

Sparkless uses robin-sparkless as an optional execution backend. This failure indicates a Robin–PySpark parity gap. Cross-reference: `docs/robin_mode_subset_report.md`, `docs/robin_improvement_plan.md` in the sparkless repo.
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create Robin parity issues from result files."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print issues only, do not call gh"
    )
    parser.add_argument(
        "--robin-results",
        type=Path,
        default=None,
        help="Path to Robin run output (default: tests/robin_parity_broad_results.txt)",
    )
    parser.add_argument(
        "--pyspark-results",
        type=Path,
        default=None,
        help="Path to PySpark run output (default: tests/pyspark_parity_failed_results.txt)",
    )
    parser.add_argument(
        "--no-already-filed",
        action="store_true",
        help="Do not subtract the 17 already-filed dataframe/functions issues (use for sql/internal batch)",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    robin_path = args.robin_results or (
        root / "tests" / "robin_parity_broad_results.txt"
    )
    pyspark_path = args.pyspark_results or (
        root / "tests" / "pyspark_parity_failed_results.txt"
    )
    if not robin_path.exists():
        print(f"Missing {robin_path}", file=sys.stderr)
        return 1
    if not pyspark_path.exists():
        print(f"Missing {pyspark_path}", file=sys.stderr)
        return 1

    robin_errors = parse_robin_failures(robin_path)
    pyspark_passed = parse_pyspark_passed(pyspark_path)
    if args.no_already_filed:
        new_tests = sorted(pyspark_passed)
        print(
            f"PySpark passed: {len(pyspark_passed)}, new (no already-filed filter): {len(new_tests)}"
        )
    else:
        new_tests = sorted(pyspark_passed - ALREADY_FILED)
        print(
            f"PySpark passed: {len(pyspark_passed)}, already filed: {len(ALREADY_FILED)}, new: {len(new_tests)}"
        )

    body_file = root / "tests" / ".robin_issue_body.txt"
    body_file.parent.mkdir(parents=True, exist_ok=True)
    created = 0
    for test_path in new_tests:
        error = robin_errors.get(test_path, "See Robin parity run output.")
        short_name = test_path.split("::")[-1] if "::" in test_path else test_path
        title = f"[Sparkless parity] {short_name}"
        body = make_body(test_path, error)
        body_file.write_text(body, encoding="utf-8")
        if args.dry_run:
            print(f"[dry-run] Would create: {title}")
            continue
        try:
            subprocess.run(
                [
                    "gh",
                    "issue",
                    "create",
                    "-R",
                    REPO,
                    "--title",
                    title,
                    "--body-file",
                    str(body_file),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            created += 1
            print(f"Created: {short_name}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create '{short_name}': {e.stderr or e}", file=sys.stderr)
        finally:
            if body_file.exists():
                body_file.unlink(missing_ok=True)

    if not args.dry_run:
        print(f"\nCreated {created} issue(s) in {REPO}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
