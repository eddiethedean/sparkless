#!/usr/bin/env python3
"""
Parse test run output, classify each FAILED/ERROR as Sparkless vs Robin parity vs Unclear.
Write robin_parity_failures.txt and sparkless_backlog_failures.txt (and grouped by category).

Usage (from repo root):
  python scripts/classify_failures_robin_sparkless.py [path_to_test_run.txt]
  Default: test_run_20260213_194551.txt in repo root.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from collections import defaultdict

# Bucket constants
SPARKLESS = "sparkless"
ROBIN_PARITY = "robin_parity"
UNCLEAR = "unclear"


def parse_failures(path: Path) -> list[tuple[str, str]]:
    """Parse FAILED and ERROR lines; return list of (test_id, error_message)."""
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    out: list[tuple[str, str]] = []
    # FAILED tests/...::... - error message   or   ERROR tests/...::... - error message
    for m in re.finditer(r"^(?:FAILED|ERROR) (tests/[^\s]+) - (.+)$", text, re.MULTILINE):
        test_id, err = m.group(1).strip(), m.group(2).strip()
        # Truncate multi-line errors to first line
        err_first = err.split("\n")[0].strip()[:500]
        out.append((test_id, err_first))
    return out


def classify(error: str, test_id: str) -> tuple[str, str]:
    """
    Classify (error, test_id) into bucket and category.
    Returns (bucket, category) where bucket is SPARKLESS | ROBIN_PARITY | UNCLEAR.
    """
    err_lower = error.lower()

    # --- Sparkless: compat layer / fixture / wrapper ---
    if "columnoperation" in err_lower and "cannot be converted" in err_lower:
        return (SPARKLESS, "expression_conversion_column_operation")
    if "literal" in err_lower and "cannot be converted to 'column'" in err_lower:
        return (SPARKLESS, "expression_conversion_literal")
    if "windowspec" in err_lower and "cannot be converted to 'window'" in err_lower:
        return (SPARKLESS, "expression_conversion_windowspec")
    if "pysortorder" in err_lower and "cannot be converted" in err_lower:
        return (SPARKLESS, "expression_conversion_pysortorder")
    if "_nacompat" in err_lower and "has no attribute 'replace'" in err_lower:
        return (SPARKLESS, "wrapper_nacompat_replace")
    if "_pysparkcompatdataframe" in err_lower and ".__getattr__" in error and "== [" in error:
        return (SPARKLESS, "wrapper_columns_callable")
    if "has no attribute 'where'" in err_lower and "dataframe" in err_lower:
        return (SPARKLESS, "wrapper_dataframe_where")
    if "has no attribute 'crossjoin'" in err_lower:
        return (SPARKLESS, "wrapper_crossjoin")
    if "unexpected keyword argument 'subset'" in err_lower and "distinct" in err_lower:
        return (SPARKLESS, "wrapper_dropduplicates_subset")
    if "argument of type 'function' is not iterable" in error:
        return (SPARKLESS, "createDataFrame_schema_or_columns")
    if "assert <function" in error and "== [" in error:
        return (SPARKLESS, "wrapper_columns_callable")
    if "stringtype" in err_lower and "integertype" in err_lower and "assert" in error:
        return (SPARKLESS, "createDataFrame_schema_inference")
    if "has no attribute '_materialized'" in err_lower:
        return (SPARKLESS, "wrapper_materialized")

    # --- Robin parity: engine / SQL / API ---
    if "sql: only select is supported" in err_lower and ("createschema" in err_lower or "createdatabase" in err_lower):
        return (ROBIN_PARITY, "sql_ddl_not_supported")
    if "py_greatest()" in error or "py_least()" in error:
        return (ROBIN_PARITY, "functions_variadic_greatest_least")
    if "py_coalesce()" in error and "takes 1 positional" in error:
        return (ROBIN_PARITY, "functions_variadic_coalesce")
    if "groupeddata.avg()" in err_lower and "takes 1 positional" in error:
        return (ROBIN_PARITY, "groupeddata_avg_multiple_cols")
    if "dataframe.agg()" in err_lower and "takes 1 positional" in error:
        return (ROBIN_PARITY, "dataframe_agg_global")
    if "join 'on' must be str or list" in error or "on=" in error and "must be str" in error:
        return (ROBIN_PARITY, "join_on_column_not_accepted")
    if "duplicate: column with name" in error and "has more than one occurrence" in error:
        return (ROBIN_PARITY, "aggregate_duplicate_column_alias")
    if "has no attribute 'eqnullsafe'" in err_lower and "column" in err_lower:
        return (ROBIN_PARITY, "column_eqnullsafe")
    if "has no attribute 'isnull'" in err_lower and "column" in err_lower:
        return (ROBIN_PARITY, "column_isnull")
    if "has no attribute 'listdatabases'" in err_lower or "has no attribute 'createdatabase'" in err_lower:
        return (ROBIN_PARITY, "catalog_api")
    if "function' object has no attribute 'mode'" in error:
        return (ROBIN_PARITY, "describe_or_show_mode")
    if "invalid column type" in err_lower and "pysortorder" in err_lower:
        return (ROBIN_PARITY, "order_by_pysortorder")
    if "argument 'ascending': 'bool' object cannot be converted to 'sequence'" in err_lower:
        return (ROBIN_PARITY, "order_by_ascending_type")
    if "argument 'partition_by': 'windowspec' object cannot be converted to 'sequence'" in err_lower:
        return (ROBIN_PARITY, "window_partition_by_type")
    if "hasthen.when' object has no attribute 'when'" in err_lower or "whenthen" in err_lower and "has no attribute 'when'" in err_lower:
        return (ROBIN_PARITY, "whenthen_when_chain")
    if "dataframes are not equivalent" in err_lower and ("isnull" in test_id or "isnotnull" in test_id or "null_handling" in test_id):
        return (ROBIN_PARITY, "result_parity_null_handling")
    if "assert '25' == 25" in error or "type coercion" in err_lower:
        return (ROBIN_PARITY, "result_parity_type_coercion")
    if "expected timestamptype, got stringtype" in err_lower or "assert 'stringtype' == 'timestamptype'" in err_lower:
        return (ROBIN_PARITY, "result_parity_schema_type")
    if "condition must be a column or literal bool" in err_lower:
        return (ROBIN_PARITY, "filter_condition_type")
    if "not found:" in error and "column" in err_lower:
        return (ROBIN_PARITY, "column_resolution")
    if "lengths don't match" in error:
        return (ROBIN_PARITY, "lengths_dont_match")

    # --- Unclear: could be Sparkless not converting or Robin not accepting ---
    if "str' object cannot be converted to 'column'" in err_lower:
        return (UNCLEAR, "str_to_column")
    if "select() items must be str" in error and "or column" in err_lower:
        return (UNCLEAR, "select_items_type")

    # --- Default Sparkless: other expression/conversion (compat passes wrong type) ---
    if "cannot be converted to 'column'" in err_lower or "cannot be converted to 'window'" in err_lower:
        return (SPARKLESS, "expression_conversion_other")
    if "cannot be converted to 'sequence'" in err_lower:
        return (ROBIN_PARITY, "argument_sequence_type")

    # Catch-all
    return (UNCLEAR, "other")


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    path = root / "test_run_20260213_194551.txt"
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    if not path.is_absolute():
        path = root / path

    failures = parse_failures(path)
    if not failures:
        print(f"No FAILED/ERROR lines found in {path}", file=sys.stderr)
        return 1

    robin: list[tuple[str, str, str]] = []  # (test_id, error, category)
    sparkless: list[tuple[str, str, str]] = []
    unclear: list[tuple[str, str, str]] = []

    for test_id, err in failures:
        bucket, category = classify(err, test_id)
        if bucket == ROBIN_PARITY:
            robin.append((test_id, err, category))
        elif bucket == SPARKLESS:
            sparkless.append((test_id, err, category))
        else:
            unclear.append((test_id, err, category))

    out_dir = root / "tests"
    out_dir.mkdir(exist_ok=True)

    def write_flat(out_path: Path, items: list[tuple[str, str, str]], header: str) -> None:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header)
            for test_id, err, cat in items:
                f.write(f"{test_id}\t{cat}\t{err[:200]}\n")

    write_flat(
        out_dir / "robin_parity_failures.txt",
        robin,
        "# test_id\tcategory\terror (first 200 chars)\n",
    )
    write_flat(
        out_dir / "sparkless_backlog_failures.txt",
        sparkless,
        "# test_id\tcategory\terror (first 200 chars)\n",
    )
    write_flat(
        out_dir / "unclear_failures.txt",
        unclear,
        "# test_id\tcategory\terror (first 200 chars)\n",
    )

    # Grouped by category for Sparkless and Robin
    def write_grouped(out_path: Path, items: list[tuple[str, str, str]], title: str) -> None:
        by_cat: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for test_id, err, cat in items:
            by_cat[cat].append((test_id, err))
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n")
            for cat in sorted(by_cat.keys()):
                f.write(f"## {cat}\n\n")
                for test_id, err in by_cat[cat]:
                    f.write(f"- {test_id}\n  {err[:150]}\n")
                f.write("\n")

    write_grouped(out_dir / "robin_parity_failures_by_category.txt", robin, "Robin parity failures by category")
    write_grouped(out_dir / "sparkless_backlog_failures_by_category.txt", sparkless, "Sparkless backlog failures by category")

    print(f"Parsed {len(failures)} failures from {path}")
    print(f"  Robin parity: {len(robin)} -> tests/robin_parity_failures.txt")
    print(f"  Sparkless:    {len(sparkless)} -> tests/sparkless_backlog_failures.txt")
    print(f"  Unclear:     {len(unclear)} -> tests/unclear_failures.txt")
    return 0


if __name__ == "__main__":
    sys.exit(main())
