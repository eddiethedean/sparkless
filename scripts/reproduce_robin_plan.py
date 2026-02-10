#!/usr/bin/env python3
"""
Load a debug plan dump (from SPARKLESS_DEBUG_PLAN_DIR) and print summary or run against Robin.

Usage:
  python scripts/reproduce_robin_plan.py [path_to_dump_dir]

If no path is given, uses tests/debug_plan_output/run_001 (or the latest run_*).
Prints plan, input rows count, and schema. If Robin exposes a plan executor in the future,
this script can be extended to call it; until then it documents what Sparkless sent.
See docs/development/debugging_plans.md.
"""

import json
import sys
from pathlib import Path


def main() -> None:
    if len(sys.argv) > 1:
        dump_dir = Path(sys.argv[1])
    else:
        base = Path(__file__).resolve().parent.parent / "tests" / "debug_plan_output"
        if not base.exists():
            print(f"Directory not found: {base}")
            print("Run a failing test with SPARKLESS_DEBUG_PLAN_DIR=tests/debug_plan_output")
            sys.exit(1)
        runs = sorted(base.glob("run_*"))
        if not runs:
            print(f"No run_* subdirs in {base}")
            sys.exit(1)
        dump_dir = runs[-1]

    if not dump_dir.is_dir():
        print(f"Not a directory: {dump_dir}")
        sys.exit(1)

    print(f"Load dump: {dump_dir}")

    input_path = dump_dir / "input_data.json"
    schema_path = dump_dir / "schema.json"
    plan_path = dump_dir / "plan.json"
    result_path = dump_dir / "result.json"
    error_path = dump_dir / "error.txt"

    if input_path.exists():
        with open(input_path) as f:
            input_data = json.load(f)
        print(f"  input_data: {len(input_data)} rows")
    else:
        print("  input_data.json: not found")

    if schema_path.exists():
        with open(schema_path) as f:
            schema = json.load(f)
        print(f"  schema: {len(schema)} fields", [s["name"] for s in schema])
    else:
        print("  schema.json: not found")

    if plan_path.exists():
        with open(plan_path) as f:
            plan = json.load(f)
        print(f"  plan: {len(plan)} operations", [p.get("op") for p in plan])
    else:
        print("  plan.json: not found")

    if result_path.exists():
        with open(result_path) as f:
            result = json.load(f)
        print(f"  result: {len(result)} rows")
    else:
        print("  result.json: not found")

    if error_path.exists():
        print("  error.txt: present (materialization failed)")
        with open(error_path) as f:
            print(f"\n{f.read()}")

    print("\nTo reproduce with Robin: use plan.json, input_data.json, schema.json.")
    print("Robin plan execution API is not yet wired in Sparkless; see docs/internal/robin_plan_contract.md.")


if __name__ == "__main__":
    main()
