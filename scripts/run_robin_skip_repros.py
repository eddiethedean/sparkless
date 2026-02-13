#!/usr/bin/env python3
"""
Run v4 skip-list repro scripts (19_* through 27_*) and print a one-line summary per gap.

Use this to confirm which parity gaps still need to be filed or which have been
fixed in a new Robin release.

Usage:
  python scripts/run_robin_skip_repros.py              # run 19-27 only
  python scripts/run_robin_skip_repros.py --all        # run all repros in robin_parity_repros/

Output: For each script, Robin PASS/FAIL and PySpark PASS/FAIL. Exit 0 if all
Robin and PySpark pass; exit 1 if any Robin FAIL (parity gap) or script error.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPROS_DIR = ROOT / "scripts" / "robin_parity_repros"

# Scripts added for v4 skip list (19-27)
SKIP_LIST_REPROS = [
    "19_spark_sql_table.py",
    "20_window_accept_str.py",
    "21_na_drop_fill.py",
    "22_fillna_subset.py",
    "23_create_df_empty_schema.py",
    "24_union_by_name_allow_missing.py",
    "25_global_agg.py",
    "26_first_ignore_nulls.py",
    "27_get_active_session_aggregate.py",
]


def run_script(script_path: Path, timeout: int = 60) -> tuple[int, str]:
    """Run script; return (exit_code, combined stdout+stderr)."""
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    out = (result.stdout or "") + (result.stderr or "")
    return result.returncode, out


def parse_result(output: str) -> tuple[str, str]:
    """Parse 'Robin: PASS/FAIL/OK/FAILED' and 'PySpark: PASS/FAIL/OK/SKIP' from script output."""
    robin = "UNKNOWN"
    pyspark = "UNKNOWN"
    for line in output.splitlines():
        if line.strip().startswith("Robin:"):
            m = re.search(r"Robin:\s*(PASS|FAIL|OK|FAILED)", line, re.I)
            if m:
                v = m.group(1).upper()
                robin = "PASS" if v in ("PASS", "OK") else "FAIL" if v in ("FAIL", "FAILED") else v
        if line.strip().startswith("PySpark:"):
            m = re.search(r"PySpark:\s*(PASS|FAIL|OK|SKIP)", line, re.I)
            if m:
                v = m.group(1).upper()
                pyspark = "PASS" if v in ("PASS", "OK") else "FAIL" if v in ("FAIL", "SKIP") else v
    return robin, pyspark


def main() -> int:
    ap = argparse.ArgumentParser(description="Run v4 skip-list repros and summarize.")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Run all scripts in robin_parity_repros/, not just 19-27",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout per script in seconds (default 60)",
    )
    args = ap.parse_args()

    if args.all:
        scripts = sorted(
            p for p in REPROS_DIR.glob("*.py") if p.name != "__init__.py"
        )
    else:
        scripts = []
        for name in SKIP_LIST_REPROS:
            p = REPROS_DIR / name
            if p.exists():
                scripts.append(p)
        if not scripts:
            print("No skip-list repro scripts (19_*.py - 27_*.py) found.", file=sys.stderr)
            return 1

    print("Script                    | Robin  | PySpark | Gap (Robin FAIL + PySpark OK)")
    print("-" * 75)
    any_robin_fail = False
    any_fail = False
    for script in scripts:
        code, out = run_script(script, timeout=args.timeout)
        robin, pyspark = parse_result(out)
        gap = "YES" if (robin == "FAIL" and pyspark == "PASS") else ""
        if robin == "FAIL":
            any_robin_fail = True
        if code != 0:
            any_fail = True
        print(f"{script.name:25} | {robin:6} | {pyspark:7} | {gap}")

    if any_robin_fail:
        print("\nOne or more Robin repros failed (parity gap). See docs/v4_robin_skip_list_to_issues.md.")
    if any_fail:
        print("\nOne or more scripts exited non-zero.")
    return 1 if (any_robin_fail or any_fail) else 0


if __name__ == "__main__":
    sys.exit(main())
