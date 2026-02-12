#!/usr/bin/env python3
"""
Create GitHub issues in eddiethedean/robin-sparkless for verified Robin–PySpark parity gaps.

Verified via scripts/robin_parity_repros/*.py: Robin fails or lacks API, PySpark passes.

Usage:
  python scripts/create_robin_issues_from_verified_repros.py [--dry-run]
  python scripts/create_robin_issues_from_verified_repros.py --write-bodies  # write markdown only

Requires: gh CLI authenticated with access to eddiethedean/robin-sparkless
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"
ROOT = Path(__file__).resolve().parent.parent

# Verified gaps that still need an issue (04/05/06 fixed in Robin 0.8, issues #248/#249/#250).
# Add new verified repros here; run with --dry-run first, then without to create.
ISSUES = [
    {
        "title": "[Parity] F.split(column, pattern, limit) — limit parameter not supported",
        "body": """## Summary

PySpark supports `F.split(column, pattern, limit)` with an optional third argument to limit the number of splits. Robin's `split` accepts only 2 arguments (column, pattern); passing a limit raises `TypeError: py_split() takes 2 positional arguments but 3 were given`.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"s": "a,b,c"}], [("s", "string")])

# TypeError: py_split() takes 2 positional arguments but 3 were given
df.select(F.split(F.col("s"), ",", 2)).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"s": "a,b,c"}])
out = df.select(F.split(F.col("s"), ",", 2)).collect()  # [Row(split(s, ,, 2)=['a', 'b,c'])]
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/07_split_limit.py
```

Robin fails with `TypeError: py_split() takes 2 positional arguments but 3 were given`. PySpark succeeds.

## Context

- Sparkless parity tests for split with limit fail when backend is Robin (e.g. `tests/parity/functions/test_split_limit_parity.py`, `tests/test_issue_328_split_limit.py`). Robin reports "not found: split(Value, ,, -1)" or similar when Sparkless translates the expression.
- Robin version: 0.8.0+ (sparkless pyproject.toml).
""",
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create Robin parity issues from verified repros"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print issue titles and bodies only; do not call gh",
    )
    parser.add_argument(
        "--write-bodies",
        action="store_true",
        help="Write issue bodies to scripts/robin_issue_bodies/ and exit",
    )
    args = parser.parse_args()

    bodies_dir = ROOT / "scripts" / "robin_issue_bodies"
    if args.write_bodies:
        bodies_dir.mkdir(parents=True, exist_ok=True)
        for i, issue in enumerate(ISSUES, 1):
            safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in issue["title"][:50])
            path = bodies_dir / f"verified_{i}_{safe}.md"
            path.write_text(issue["body"], encoding="utf-8")
            print(f"Wrote {path}")
        return 0

    body_file = ROOT / "tests" / ".robin_issue_body.txt"
    body_file.parent.mkdir(parents=True, exist_ok=True)
    created = 0
    for i, issue in enumerate(ISSUES, 1):
        title = issue["title"]
        body = issue["body"]
        print(f"\n--- Issue {i}: {title[:70]}...")
        if args.dry_run:
            print(body[:400] + "..." if len(body) > 400 else body)
            continue
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
