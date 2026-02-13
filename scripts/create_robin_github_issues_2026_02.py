#!/usr/bin/env python3
"""
Create GitHub issues in eddiethedean/robin-sparkless for improvements and bugs
found during Sparkless Robin mode integration (2026-02-06 analysis).

Run from repo root:
  python scripts/create_robin_github_issues_2026_02.py [--dry-run]

Requires: gh CLI authenticated with access to eddiethedean/robin-sparkless
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"

ISSUES = [
    {
        "title": "[Enhancement] Add Python operator overloads to Column for PySpark compatibility",
        "labels": [],
        "body": """## Summary

The `Column` class in robin-sparkless Python bindings does not implement Python's comparison operator overloads (`__gt__`, `__lt__`, `__ge__`, `__le__`, `__eq__`, `__ne__`). This causes `TypeError` when users (or libraries like Sparkless) write PySpark-style expressions such as `col("age") > lit(30)`.

PySpark's Column supports both:
- **Operator style:** `col("age") > lit(30)` 
- **Method style:** `col("age").gt(lit(30))`

robin-sparkless currently only supports the method style. Adding operator overloads would improve PySpark compatibility and reduce integration friction.

## Current behavior

```python
import robin_sparkless as rs
F = rs

# Method style - works
expr = F.col("age").gt(F.lit(30))  # OK

# Operator style - raises TypeError
expr = F.col("age") > F.lit(30)
# TypeError: '>' not supported between instances of 'builtins.Column' and 'builtins.Column'
```

## Expected behavior

Both styles should work:

```python
# Operator style (PySpark compatible)
expr = F.col("age") > F.lit(30)   # Should return Column, not raise

# With scalar on right (convenience)
expr = F.col("age") > 30          # Could implicitly wrap in lit() for PySpark parity
```

## Suggested implementation

Add to the Python `Column` class (PyO3 bindings):

- `__gt__(self, other)` → delegate to `self.gt(other)` or `self.gt(lit(other))`
- `__lt__(self, other)` → delegate to `self.lt(other)` or `self.lt(lit(other))`
- `__ge__(self, other)` → delegate to `self.ge(other)` or `self.ge(lit(other))`
- `__le__(self, other)` → delegate to `self.le(other)` or `self.le(lit(other))`
- `__eq__(self, other)` → delegate to `self.eq(other)` or `self.eq(lit(other))`
- `__ne__(self, other)` → delegate to `self.ne(other)` or `self.ne(lit(other))`

When `other` is a scalar (int, float, str, etc.), wrap in `lit()` before calling the method.

## Context

- **Source:** Sparkless project uses robin-sparkless as an optional backend
- **Analysis:** [sparkless/docs/robin_sparkless_ownership_analysis.md](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_ownership_analysis.md)
- **Workaround:** Sparkless was updated to use `.gt()`, `.lt()`, etc. instead of operators when calling Robin; upstream support would benefit all PySpark-style integrations.
""",
    },
    {
        "title": "[Enhancement] Join on= parameter: accept string for single column (PySpark compatibility)",
        "labels": [],
        "body": """## Summary

PySpark's `DataFrame.join(other, on="col_name", how="inner")` accepts a string when joining on a single column. robin-sparkless currently requires a list: `on=["col_name"]`. Passing `on="col_name"` raises:

```
TypeError: argument 'on': Can't extract `str` to `Vec`
```

## Current behavior

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("test").get_or_create()
df1 = spark.create_dataframe_from_rows([{"id": 1, "x": 10}], [("id", "bigint"), ("x", "bigint")])
df2 = spark.create_dataframe_from_rows([{"id": 1, "y": 20}], [("id", "bigint"), ("y", "bigint")])

# Fails - expects list
result = df1.join(df2, on="id", how="inner")
# TypeError: argument 'on': Can't extract `str` to `Vec`

# Works - must use list
result = df1.join(df2, on=["id"], how="inner")
```

## Expected (PySpark compatible)

```python
# Both should work
df1.join(df2, on="id", how="inner")    # string for single column
df1.join(df2, on=["id", "other"], how="inner")  # list for multiple columns
```

## Suggested fix

In the Python bindings for `join`, accept `on` as:
- `str` → treat as `[str]` (single column)
- `list`/`tuple` of str → use as-is

## Context

- **Source:** Sparkless (v4 uses Robin as engine); PySpark-style join APIs often pass `on="col"` for single-column joins
- **Workaround:** Callers can wrap: `on=[col] if isinstance(col, str) else col`
""",
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create GitHub issues in robin-sparkless"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print issue content without creating"
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    created = 0

    for i, issue in enumerate(ISSUES, 1):
        title = issue["title"]
        body = issue["body"]
        labels = issue.get("labels", [])

        print(f"\n--- Issue {i}: {title} ---")
        if args.dry_run:
            print(body[:500] + "..." if len(body) > 500 else body)
            print(f"\nLabels: {labels}")
            continue

        body_file = root / "tests" / ".robin_issue_body.txt"
        body_file.parent.mkdir(parents=True, exist_ok=True)
        body_file.write_text(body, encoding="utf-8")

        cmd = [
            "gh",
            "issue",
            "create",
            "-R",
            REPO,
            "--title",
            title,
            "--body-file",
            str(body_file),
        ]
        if labels:
            for label in labels:
                cmd.extend(["--label", label])

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            created += 1
            if result.stdout:
                print(f"Created: {result.stdout.strip()}")
            else:
                print(f"Created: {title[:60]}...")
        except subprocess.CalledProcessError as e:
            print(f"Failed: {e.stderr or str(e)}", file=sys.stderr)
        finally:
            if body_file.exists():
                body_file.unlink(missing_ok=True)

    if not args.dry_run:
        print(f"\nCreated {created}/{len(ISSUES)} issue(s) in {REPO}.")
    else:
        print(
            f"\nDry run: would create {len(ISSUES)} issue(s). Run without --dry-run to create."
        )

    return 0 if created == len(ISSUES) or args.dry_run else 1


if __name__ == "__main__":
    sys.exit(main())
