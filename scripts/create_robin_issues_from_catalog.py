#!/usr/bin/env python3
"""
Build failure catalog from Robin unit+integration and parity runs, classify,
group by root cause, and create one GitHub issue per group in eddiethedean/robin-sparkless.

Run from repo root:
  python scripts/create_robin_issues_from_catalog.py --dry-run   # preview
  python scripts/create_robin_issues_from_catalog.py             # create issues

Requires: tests/robin_unit_integration_results.txt, tests/robin_parity_full_results.txt,
          tests/pyspark_parity_full_results.txt (from plan step 1-2).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO = "eddiethedean/robin-sparkless"

# Known existing issues (from docs/robin_sparkless_issues.md) — do not create duplicates.
EXISTING_ISSUES_COVER = {
    "select/with_column expression evaluation": ["#182"],
    "filter Column–Column comparison": ["#184"],
    "filter bool condition": ["#185"],
    "lit date/datetime": ["#186"],
    "Window API": ["#187"],
    "Column operator overloads": ["#174"],
    "Join on= single string": ["#175"],
    "Stability / xdist fork": ["#178"],
}


def parse_failures(path: Path, test_prefix: str = "tests/") -> dict[str, str]:
    """Parse FAILED lines; map test_id -> first line of error. Optional prefix filter."""
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for m in re.finditer(r"^FAILED (tests/[^\s]+) - (.+)$", text, re.MULTILINE):
        test_id, err = m.group(1).strip(), m.group(2).strip()
        if not test_id.startswith(test_prefix) and test_prefix != "tests/":
            continue
        out[test_id] = err[:500]
    return out


def parse_passed(path: Path, test_prefix: str = "tests/parity/") -> set[str]:
    """Collect test IDs that show PASSED."""
    if not path.exists():
        return set()
    text = path.read_text(encoding="utf-8")
    passed = set()
    for m in re.finditer(r"^(tests/parity/[^\s]+) PASSED ", text, re.MULTILINE):
        passed.add(m.group(1).strip())
    return passed


def classify(error: str, test_id: str) -> str:
    """Map (error, test_id) to a category for grouping."""
    err_lower = error.lower()
    if "row_number() over" in error or "over (windowspec" in err_lower or "window" in err_lower and "test_window" in test_id:
        return "window_expressions"
    if "case when" in err_lower or "casewhen" in err_lower:
        return "casewhen"
    if "concat(" in error or "concat_ws" in err_lower:
        return "concat_concat_ws"
    if "not found: name" in error or "not found: id" in error or "unable to find column" in error and "valid columns" in error:
        if "case" in test_id or "case_sensitivity" in test_id or "case_insensitive" in err_lower:
            return "case_sensitivity"
        return "column_resolution"
    if "is_case_sensitive" in error or "case_sensitive" in err_lower:
        return "case_sensitivity"
    if "sparkunsupportedoperationerror" in err_lower and "filter" in err_lower:
        return "unsupported_filter"
    if "sparkunsupportedoperationerror" in err_lower and "join" in err_lower:
        return "unsupported_join"
    if "sparkunsupportedoperationerror" in err_lower and ("withcolumn" in err_lower or "select" in err_lower):
        return "unsupported_select_withcolumn"
    if "arithmetic on string and numeric" in error or "type strictness" in err_lower:
        return "type_strictness"
    if "not found: partial" in error or "substr" in test_id or "substring" in err_lower:
        return "substring_alias_or_partial"
    if "inferschema" in test_id or "infer_schema" in test_id or "should be longtype" in error:
        return "infer_schema"
    if "datetime is not json serializable" in error:
        return "sparkless_test_datetime_json"
    if "map()" in error or "array()" in error or "row values must be" in error:
        return "map_array_struct"
    if "parquet" in test_id or "storage" in error or "_storage" in error or "table_append" in test_id:
        return "sparkless_storage_parquet"
    if "unsupported backend type: pyspark" in error:
        return "sparkless_pyspark_backend"
    if "attributeerror" in err_lower or "valueerror" in err_lower and "backend" in err_lower:
        return "sparkless_test_env"
    return "other_expression_or_parity"


def make_group_body(
    title: str,
    category: str,
    test_errors: list[tuple[str, str]],
    verification: str,
) -> str:
    """Build issue body for a group."""
    tests_blob = "\n".join(
        f"- `{tid}` — {err[:120]}..." if len(err) > 120 else f"- `{tid}` — {err}"
        for tid, err in test_errors[:30]
    )
    if len(test_errors) > 30:
        tests_blob += f"\n- ... and {len(test_errors) - 30} more."
    return f"""## Summary

{title}

## Verification

{verification}

## Category

{category}

## Affected tests (Sparkless)

{tests_blob}

## How to reproduce

From the [sparkless](https://github.com/eddiethedean/sparkless) repo (with `robin-sparkless` installed):

```bash
SPARKLESS_TEST_BACKEND=robin SPARKLESS_BACKEND=robin python -m pytest <test_id> -v --tb=short
```

## Context

- [v4 behavior changes and known differences](https://github.com/eddiethedean/sparkless/blob/main/docs/v4_behavior_changes_and_known_differences.md)
- [robin-sparkless needs](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_needs.md)
- [Verification table](https://github.com/eddiethedean/sparkless/blob/main/docs/robin_sparkless_verification_table.md)
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Catalog Robin failures and create grouped gh issues.")
    parser.add_argument("--dry-run", action="store_true", help="Print only, do not call gh")
    parser.add_argument("--unit-results", type=Path, default=None)
    parser.add_argument("--parity-robin-results", type=Path, default=None)
    parser.add_argument("--parity-pyspark-results", type=Path, default=None)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent

    unit_path = args.unit_results or root / "tests" / "robin_unit_integration_results.txt"
    parity_robin_path = args.parity_robin_results or root / "tests" / "robin_parity_full_results.txt"
    parity_pyspark_path = args.parity_pyspark_results or root / "tests" / "pyspark_parity_full_results.txt"

    unit_failures = parse_failures(unit_path, "tests/")
    parity_robin = parse_failures(parity_robin_path, "tests/parity/")
    pyspark_passed = parse_passed(parity_pyspark_path)

    parity_gap = {tid for tid in parity_robin if tid in pyspark_passed}

    # Combine unit+integration and parity for grouping; tag parity_gap
    all_entries: list[tuple[str, str, bool]] = []
    for tid, err in unit_failures.items():
        all_entries.append((tid, err, False))
    for tid, err in parity_robin.items():
        if tid not in unit_failures:
            all_entries.append((tid, err, tid in pyspark_passed))

    # Classify and group
    groups: dict[str, list[tuple[str, str]]] = {}
    for tid, err, is_parity_gap in all_entries:
        cat = classify(err, tid)
        if cat not in groups:
            groups[cat] = []
        groups[cat].append((tid, err))

    # Skip Sparkless-only / test env / known-covered
    skip_categories = {
        "sparkless_test_datetime_json",
        "sparkless_storage_parquet",
        "sparkless_pyspark_backend",
        "sparkless_test_env",
    }
    category_to_existing = {
        "window_expressions": "Window API",
        "casewhen": "select/with_column expression evaluation",
        "concat_concat_ws": None,
        "case_sensitivity": None,
        "unsupported_filter": None,
        "unsupported_join": "Join on= single string",
        "unsupported_select_withcolumn": "select/with_column expression evaluation",
        "type_strictness": None,
        "substring_alias_or_partial": None,
        "infer_schema": None,
        "map_array_struct": None,
        "column_resolution": None,
        "other_expression_or_parity": None,
    }

    titles = {
        "window_expressions": "[Sparkless parity] Window expressions in select/withColumn (row_number, sum over window, etc.)",
        "casewhen": "[Sparkless parity] CaseWhen / when().then().otherwise() in select/withColumn",
        "concat_concat_ws": "[Sparkless parity] concat/concat_ws with literal separator or mixed literals",
        "case_sensitivity": "[Sparkless parity] Column name case sensitivity vs PySpark",
        "unsupported_filter": "[Sparkless parity] Unsupported filter conditions (complex/column-column)",
        "unsupported_join": "[Sparkless parity] Join with complex on or unsupported join",
        "unsupported_select_withcolumn": "[Sparkless parity] Unsupported select/withColumn expressions",
        "type_strictness": "[Sparkless parity] Type strictness (string vs numeric, coercion)",
        "substring_alias_or_partial": "[Sparkless parity] substring/substr or alias/partial resolution",
        "infer_schema": "[Sparkless] Reader schema inference (string-only in v4)",
        "map_array_struct": "[Sparkless parity] map(), array(), nested struct/row values",
        "column_resolution": "[Sparkless parity] Column/expression resolution (not found)",
        "other_expression_or_parity": "[Sparkless parity] Other expression or result parity",
    }

    verification_default = "Confirmed from Sparkless test run; see docs/robin_sparkless_verification_table.md for robin-sparkless feature check."

    created = 0
    for cat, entries in sorted(groups.items()):
        if cat in skip_categories or not entries:
            continue
        existing_key = category_to_existing.get(cat)
        if existing_key and EXISTING_ISSUES_COVER.get(existing_key):
            continue
        title = titles.get(cat, f"[Sparkless parity] {cat}")
        body = make_group_body(title, cat, entries, verification_default)
        if args.dry_run:
            print(f"[dry-run] Would create: {title} ({len(entries)} tests)")
            continue
        body_file = root / "tests" / ".robin_group_issue_body.txt"
        body_file.write_text(body, encoding="utf-8")
        try:
            subprocess.run(
                ["gh", "issue", "create", "-R", REPO, "--title", title, "--body-file", str(body_file)],
                check=True,
                capture_output=True,
                text=True,
            )
            created += 1
            print(f"Created: {title}")
        except subprocess.CalledProcessError as e:
            print(f"Failed: {e.stderr or e}", file=sys.stderr)
        finally:
            if body_file.exists():
                body_file.unlink(missing_ok=True)

    if not args.dry_run:
        print(f"\nCreated {created} issue(s) in {REPO}.")
    else:
        print(f"\n[dry-run] Would create {len([c for c in groups if c not in skip_categories and not (category_to_existing.get(c) and EXISTING_ISSUES_COVER.get(category_to_existing.get(c)))])} issue(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
