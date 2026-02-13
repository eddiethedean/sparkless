#!/usr/bin/env python3
"""
Create detailed GitHub issues in eddiethedean/robin-sparkless for each failed
Sparkless parity test when run with Robin backend. Uses `gh` CLI.

Run from repo root:
  python scripts/create_robin_github_issues.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"

FAILED_TESTS = [
    # Join (6)
    (
        "test_join.py::TestJoinParity::test_inner_join",
        "Join (inner) returns 0 rows",
        "Row count mismatch: mock=0, expected=3",
        "Inner join on dept_id between employees (4 rows) and departments (3 rows) should return 3 rows (matches on dept_id 10, 20).",
        "Result has 0 rows when using robin-sparkless backend.",
    ),
    (
        "test_join.py::TestJoinParity::test_left_join",
        "Join (left) returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "Left join should return 4 rows (all left side).",
        "Result has 0 rows.",
    ),
    (
        "test_join.py::TestJoinParity::test_right_join",
        "Join (right) returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "Right join should return 4 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_join.py::TestJoinParity::test_outer_join",
        "Join (outer) returns 0 rows",
        "Row count mismatch: mock=0, expected=5",
        "Full outer join should return 5 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_join.py::TestJoinParity::test_semi_join",
        "Join (left_semi) returns 0 rows",
        "Row count mismatch: mock=0, expected=3",
        "Left semi join should return 3 rows (left rows that match).",
        "Result has 0 rows.",
    ),
    (
        "test_join.py::TestJoinParity::test_anti_join",
        "Join (left_anti) returns 0 rows",
        "Row count mismatch: mock=0, expected=1",
        "Left anti join should return 1 row (left row with no match).",
        "Result has 0 rows.",
    ),
    # Filter (5)
    (
        "test_filter.py::TestFilterParity::test_filter_operations",
        "Filter (simple) returns 0 rows",
        "Row count mismatch: mock=0, expected=2",
        "Filter (age > 30) should return 2 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_filter.py::TestFilterParity::test_filter_with_boolean",
        "Filter (boolean) returns 0 rows",
        "Row count mismatch: mock=0, expected=2",
        "Filter (salary > 60000) should return 2 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_filter.py::TestFilterParity::test_filter_with_and_operator",
        "Filter with AND (Column & Column) raises TypeError",
        "TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'",
        "Filter (F.col('a') > 1) & (F.col('b') > 1) should return 1 row.",
        "Comparison or sort path compares Column objects; may be in test harness or robin result handling.",
    ),
    (
        "test_filter.py::TestFilterParity::test_filter_with_or_operator",
        "Filter with OR (Column | Column) raises TypeError",
        "TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'",
        "Filter (F.col('a') > 1) | (F.col('b') > 1) should return 3 rows.",
        "Same as AND: Column vs Column comparison.",
    ),
    (
        "test_filter.py::TestFilterParity::test_filter_on_table_with_complex_schema",
        "Table read with complex schema returns wrong row count",
        "Table read should return 3 rows (got 6)",
        "After writing 3 rows to a table (29-column schema) and reading back, count should be 3.",
        "read_df.count() returns 6 instead of 3 (duplication or storage behavior).",
    ),
    # Select (2)
    (
        "test_select.py::TestSelectParity::test_select_with_alias",
        "Select with alias returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "Select with column alias should return 4 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_select.py::TestSelectParity::test_column_access",
        "Select (column access) returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "Select by column access should return 4 rows.",
        "Result has 0 rows.",
    ),
    # Transformations (4)
    (
        "test_transformations.py::TestTransformationsParity::test_with_column",
        "withColumn returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "withColumn should return 4 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_transformations.py::TestTransformationsParity::test_drop_column",
        "drop column returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "drop should return 4 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_transformations.py::TestTransformationsParity::test_distinct",
        "distinct returns 0 rows",
        "Row count mismatch: mock=0, expected=3",
        "distinct should return 3 rows.",
        "Result has 0 rows.",
    ),
    (
        "test_transformations.py::TestTransformationsParity::test_order_by_desc",
        "orderBy desc returns 0 rows",
        "Row count mismatch: mock=0, expected=4",
        "orderBy(..., ascending=False) should return 4 rows.",
        "Result has 0 rows.",
    ),
]


def make_body(test_path: str, error: str, expected: str, actual: str) -> str:
    return f"""## Summary

When [Sparkless](https://github.com/eddiethedean/sparkless) runs its parity test suite with the **Robin backend** (robin-sparkless), this test fails. The same test **passes** with the PySpark backend.

## Test

- **Suite:** Sparkless parity tests (`tests/parity/dataframe/`)
- **Test:** `{test_path}`

## Error

```
{error}
```

## Expected

{expected}

## Actual

{actual}

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo (with `robin-sparkless` installed):

```bash
pip install robin-sparkless
cd /path/to/sparkless
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest \\
  tests/parity/dataframe/{test_path.split("::")[0]}::{test_path.split("::")[1]}::{test_path.split("::")[2]} \\
  -v --tb=short
```

## Context

Sparkless v4 uses robin-sparkless as the execution engine and calls its APIs (`create_dataframe_from_rows`, `filter`, `select`, `join`, `collect`, etc.). This failure may indicate:

- Incorrect result from a robin-sparkless API (e.g. `join` or `collect` returning empty/wrong rows)
- API contract mismatch (e.g. parameter format for `on=` or `how=`)
- Schema or row serialization difference

Cross-reference: Sparkless Robin mode subset report and improvement plan in the sparkless repo (`docs/robin_mode_subset_report.md`, `docs/robin_improvement_plan.md`).
"""


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    created = 0
    for test_path, title_short, error, expected, actual in FAILED_TESTS:
        title = f"[Sparkless parity] {title_short}"
        body = make_body(test_path, error, expected, actual)
        body_file = root / "tests" / ".robin_issue_body.txt"
        body_file.parent.mkdir(parents=True, exist_ok=True)
        body_file.write_text(body, encoding="utf-8")
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
            print(f"Created: {title_short}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create '{title_short}': {e.stderr or e}", file=sys.stderr)
        finally:
            if body_file.exists():
                body_file.unlink()
    print(f"\nCreated {created} issue(s) in {REPO}.")
    return 0 if created == len(FAILED_TESTS) else 1


if __name__ == "__main__":
    sys.exit(main())
