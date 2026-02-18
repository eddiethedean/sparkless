#!/usr/bin/env python3
"""
Create new robin-sparkless GitHub issues for PySpark parity (with Robin + PySpark reproductions).

Run from repo root:
  python scripts/create_robin_issues_parity_new.py [--dry-run] [--issue N|all]

Requires: gh CLI authenticated with access to eddiethedean/robin-sparkless
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"
DOCS = Path(__file__).resolve().parent.parent / "docs"

ISSUES = [
    {
        "num": 1,
        "title": "[PySpark parity] Unsupported expression ops: create_map, getItem, regexp_extract, startswith, is_null",
        "body_file": DOCS / "robin_github_issue_unsupported_expression_ops.md",
    },
    {
        "num": 2,
        "title": "[PySpark parity] Unsupported function / op: isin",
        "body_file": DOCS / "robin_github_issue_unsupported_isin.md",
    },
    {
        "num": 3,
        "title": "[PySpark parity] Window expressions in select (expression must have col/lit/op/fn)",
        "body_file": DOCS / "robin_github_issue_window_expressions_in_select.md",
    },
    {
        "num": 4,
        "title": "[PySpark parity] Case sensitivity and column-not-found",
        "body_file": DOCS / "robin_github_issue_case_sensitivity.md",
    },
    {
        "num": 5,
        "title": "[PySpark parity] Empty DataFrame with schema + parquet table append / catalog",
        "body_file": DOCS / "robin_github_issue_empty_df_parquet_append.md",
    },
]


def create_issue(issue: dict, dry_run: bool) -> bool:
    title = issue["title"]
    body_file = issue["body_file"]
    if not body_file.exists():
        print(f"Body file not found: {body_file}", file=sys.stderr)
        return False
    if dry_run:
        print(f"[dry-run] Would create: {title[:60]}...")
        print(f"  Body: {body_file}")
        return True
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
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout.strip() or f"Created: {title[:50]}...")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Failed: {e.stderr or str(e)}", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create new robin-sparkless GitHub issues for PySpark parity (with reproductions)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print only, do not create"
    )
    parser.add_argument(
        "--issue",
        choices=["1", "2", "3", "4", "5", "all"],
        default="all",
        help="Which issue to create (default: all)",
    )
    args = parser.parse_args()

    if args.issue == "all":
        to_create = ISSUES
    else:
        n = int(args.issue)
        to_create = [i for i in ISSUES if i["num"] == n]

    ok = True
    for issue in to_create:
        if not create_issue(issue, args.dry_run):
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
