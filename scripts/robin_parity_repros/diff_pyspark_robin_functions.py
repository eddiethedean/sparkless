#!/usr/bin/env python3
"""
Optional API surface diff: list names in pyspark.sql.functions that are not
exposed (or not callable) in robin_sparkless. Run from repo root:
  python scripts/robin_parity_repros/diff_pyspark_robin_functions.py

Use output to prioritize which functions to add to the checklist and repro.
"Exists" in Robin does not mean same behavior; still run minimal repros.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    try:
        from pyspark.sql import functions as PF
    except ImportError as e:
        print("PySpark not available:", e, file=sys.stderr)
        sys.exit(1)
    try:
        import robin_sparkless as RS
    except ImportError as e:
        print("robin_sparkless not available:", e, file=sys.stderr)
        sys.exit(1)

    # PySpark: public callables or common F.* names (skip deprecated/private)
    pyspark_names = set()
    for name in dir(PF):
        if name.startswith("_"):
            continue
        obj = getattr(PF, name)
        if callable(obj) or (getattr(obj, "__doc__", None) and "Column" in str(type(obj))):
            pyspark_names.add(name)
    if hasattr(PF, "__all__"):
        pyspark_names |= set(getattr(PF, "__all__"))

    # Robin: module-level attributes that are callable or Column-like
    robin_names = set()
    for name in dir(RS):
        if name.startswith("_"):
            continue
        robin_names.add(name)

    only_pyspark = sorted(pyspark_names - robin_names)
    only_robin = sorted(robin_names - pyspark_names)

    print("Names in pyspark.sql.functions but not in robin_sparkless (candidates for repro):")
    for name in only_pyspark[:80]:
        print(" ", name)
    if len(only_pyspark) > 80:
        print("  ... and", len(only_pyspark) - 80, "more")
    print("\nTotal only in PySpark:", len(only_pyspark))

    if only_robin:
        print("\nNames in robin_sparkless but not in pyspark.sql.functions (sample):")
        for name in only_robin[:20]:
            print(" ", name)
        if len(only_robin) > 20:
            print("  ... and", len(only_robin) - 20, "more")


if __name__ == "__main__":
    main()
