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

ISSUES = [
    {
        "title": "[Parity] Column.eqNullSafe / eq_null_safe missing for null-safe equality in filter",
        "body": """## Summary

PySpark's `Column` has `eqNullSafe(other)` (null-safe equality: NULL <=> NULL is true). robin-sparkless Column does not expose `eq_null_safe` or `eqNullSafe`, so filter expressions using null-safe equality cannot be expressed when using Robin directly.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"a": 1}, {"a": None}, {"a": 3}], [("a", "int")])

# AttributeError or similar: eq_null_safe / eqNullSafe not found
df.filter(F.col("a").eq_null_safe(F.lit(None))).collect()
```

Robin Column has no `eq_null_safe` or `eqNullSafe` method.

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"a": 1}, {"a": None}, {"a": 3}])
out = df.filter(F.col("a").eqNullSafe(F.lit(None))).collect()  # 1 row (the None row)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo (with robin-sparkless and optionally pyspark installed):

```bash
python scripts/robin_parity_repros/04_filter_eqNullSafe.py
```

Robin reports: `eq_null_safe / eqNullSafe not found`. PySpark path succeeds.

## Context

- Sparkless uses robin-sparkless as execution backend. Many Sparkless tests fail with "Operation 'Operations: filter' is not supported" when the filter uses eqNullSafe; the Sparkless Robin materializer cannot translate it because Robin Column lacks this method.
- Representative Sparkless test: `tests/test_issue_260_eq_null_safe.py`
""",
    },
    {
        "title": "[Parity] soundex() string function missing",
        "body": """## Summary

PySpark provides `F.soundex(col)` for phonetic encoding of strings. robin-sparkless does not expose a `soundex` function, so string parity tests (e.g. soundex) fail when run with Robin backend (e.g. 0 rows vs expected 3).

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}], [("name", "string")])

# F.soundex not found
df.with_column("snd", F.soundex(F.col("name"))).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}])
out = df.withColumn("snd", F.soundex(F.col("name"))).collect()  # 3 rows with snd column
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/05_parity_string_soundex.py
```

Robin reports: `F.soundex not found`. PySpark path succeeds.

## Context

- Sparkless parity test: `tests/parity/functions/test_string.py::TestStringFunctionsParity::test_soundex` fails with AssertionError (DataFrames not equivalent / row count mismatch) when backend is Robin because soundex is not available.
""",
    },
    {
        "title": "[Parity] Column.between(low, high) missing for range filter",
        "body": """## Summary

PySpark's `Column` has `between(low, high)` for inclusive range filters. robin-sparkless Column does not expose `between`, so filter expressions using between cannot be expressed when using Robin directly.

## Current behavior (Robin)

```python
import robin_sparkless as rs
F = rs
spark = F.SparkSession.builder().app_name("test").get_or_create()
create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(spark, "_create_dataframe_from_rows")
df = create_df([{"v": 10}, {"v": 25}, {"v": 50}], [("v", "int")])

# AttributeError: col.between not found
df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()
```

## Expected behavior (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
df = spark.createDataFrame([{"v": 10}, {"v": 25}, {"v": 50}])
out = df.filter(F.col("v").between(F.lit(20), F.lit(30))).collect()  # 1 row (v=25)
```

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo:

```bash
python scripts/robin_parity_repros/06_filter_between.py
```

Robin reports: `col.between not found`. PySpark path succeeds.

## Context

- Sparkless tests fail with "Operation 'Operations: filter' is not supported" when the filter uses between(); the Sparkless Robin materializer cannot translate it because Robin Column lacks this method.
- Representative Sparkless test: `tests/test_issue_261_between.py`
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
