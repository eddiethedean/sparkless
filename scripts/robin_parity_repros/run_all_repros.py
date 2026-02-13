#!/usr/bin/env python3
"""Run all repro scripts and print summary. Updates VERIFICATION.md from results."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
REPROS_DIR = Path(__file__).resolve().parent


def main() -> int:
    scripts = sorted(
        p for p in REPROS_DIR.glob("*.py")
        if p.name != "__init__.py"
    )
    if not scripts:
        print("No 0*.py repro scripts found", file=sys.stderr)
        return 1
    for script in scripts:
        print(f"\n=== {script.name} ===")
        subprocess.run(
            [sys.executable, str(script)],
            cwd=str(ROOT),
            timeout=60,
        )
    print("\nSee VERIFICATION.md for verified Robin issues.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
